from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Annotated, Any
from unittest.mock import MagicMock

from fastapi import BackgroundTasks, HTTPException, Query

from diracx.core.models import (
    JobMinorStatus,
    JobStatus,
    JobStatusUpdate,
    SetJobStatusReturn,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.job_logging.db import JobLoggingRecord

from ..dependencies import (
    Config,
    JobDB,
    JobLoggingDB,
    SandboxMetadataDB,
    TaskQueueDB,
)
from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckWMSPolicyCallable

logger = logging.getLogger(__name__)

router = DiracxRouter()


async def _reschedule_jobs_bulk(
    job_ids: list[int],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    *,
    reset_counter=False,
) -> dict[str, Any]:
    """Reschedule given job."""
    from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
    from DIRAC.Core.Utilities.ReturnValues import SErrorException

    failed = {}
    reschedule_max = config.Operations[
        "Defaults"
    ].Services.JobScheduling.MaxRescheduling  # type: ignore

    status_changes = {}
    attribute_changes: dict[int, dict[str, str]] = defaultdict(dict)
    jdl_changes = {}

    _, results = await job_db.search(
        parameters=[
            "Status",
            "MinorStatus",
            "VerifiedFlag",
            "RescheduleCounter",
            "Owner",
            "OwnerGroup",
            "JobID",
        ],
        search=[
            VectorSearchSpec(
                parameter="JobID", operator=VectorSearchOperator.IN, values=job_ids
            )
        ],
        sorts=[],
    )
    if not results:
        for job_id in job_ids:
            failed[job_id] = {"detail": "Not found"}

    jobs_to_resched = {}

    for job_attrs in results or []:
        job_id = int(job_attrs["JobID"])

        if "VerifiedFlag" not in job_attrs:
            failed[job_id] = {"detail": "Not found: No verified flag"}
            # Noop
            continue

        if not job_attrs["VerifiedFlag"]:
            failed[job_id] = {
                "detail": (
                    f"VerifiedFlag is False: Status {job_attrs['Status']}, "
                    f"Minor Status: {job_attrs['MinorStatus']}"
                )
            }
            # Noop
            continue

        if reset_counter:
            job_attrs["RescheduleCounter"] = 0
        else:
            job_attrs["RescheduleCounter"] = int(job_attrs["RescheduleCounter"]) + 1

        if job_attrs["RescheduleCounter"] > reschedule_max:
            status_changes[job_id] = {
                datetime.now(tz=timezone.utc): JobStatusUpdate(
                    Status=JobStatus.FAILED,
                    MinorStatus=JobMinorStatus.MAX_RESCHEDULING,
                    ApplicationStatus="Unknown",
                )
            }
            failed[job_id] = {
                "detail": f"Maximum number of reschedules exceeded ({reschedule_max})"
            }
            # DATABASE OPERATION (status change)
            continue
        jobs_to_resched[job_id] = job_attrs

    surviving_job_ids = set(jobs_to_resched.keys())

    # TODO: get the job parameters from JobMonitoringClient
    # result = JobMonitoringClient().getJobParameters(jobID)
    # if result["OK"]:
    #     parDict = result["Value"]
    #     for key, value in parDict.get(jobID, {}).items():
    #         result = self.setAtticJobParameter(jobID, key, value, rescheduleCounter - 1)
    #         if not result["OK"]:
    #             break

    # TODO: IF we keep JobParameters and OptimizerParameters: Delete job in those tables.
    # await self.delete_job_parameters(job_id)
    # await self.delete_job_optimizer_parameters(job_id)

    def parse_jdl(job_id, job_jdl):
        if not job_jdl.strip().startswith("["):
            job_jdl = f"[{job_jdl}]"
        class_ad_job = ClassAd(job_jdl)
        class_ad_job.insertAttributeInt("JobID", job_id)
        return class_ad_job

    job_jdls = {
        jobid: parse_jdl(jobid, jdl)
        for jobid, jdl in (
            (await job_db.getJobJDLs(surviving_job_ids, original=True)).items()
        )
    }

    for job_id in surviving_job_ids:
        class_ad_job = job_jdls[job_id]
        class_ad_req = ClassAd("[]")
        try:
            await job_db.checkAndPrepareJob(
                job_id,
                class_ad_job,
                class_ad_req,
                jobs_to_resched[job_id]["Owner"],
                jobs_to_resched[job_id]["OwnerGroup"],
                {"RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"]},
                class_ad_job.getAttributeString("VirtualOrganization"),
            )
        except SErrorException as e:
            failed[job_id] = {"detail": str(e)}
            # surviving_job_ids.remove(job_id)
            continue

        priority = class_ad_job.getAttributeInt("Priority")
        if priority is None:
            priority = 0

        site_list = class_ad_job.getListFromExpression("Site")
        if not site_list:
            site = "ANY"
        elif len(site_list) > 1:
            site = "Multiple"
        else:
            site = site_list[0]

        reqJDL = class_ad_req.asJDL()
        class_ad_job.insertAttributeInt("JobRequirements", reqJDL)
        jobJDL = class_ad_job.asJDL()
        # Replace the JobID placeholder if any
        jobJDL = jobJDL.replace("%j", str(job_id))

        additional_attrs = {
            "Site": site,
            "UserPriority": priority,
            "RescheduleTime": datetime.now(tz=timezone.utc),
            "RescheduleCounter": jobs_to_resched[job_id]["RescheduleCounter"],
        }

        # set new JDL
        jdl_changes[job_id] = jobJDL

        # set new status
        status_changes[job_id] = {
            datetime.now(tz=timezone.utc): JobStatusUpdate(
                Status=JobStatus.RECEIVED,
                MinorStatus=JobMinorStatus.RESCHEDULED,
                ApplicationStatus="Unknown",
            )
        }
        # set new attributes
        attribute_changes[job_id].update(additional_attrs)

    if surviving_job_ids:
        # BULK STATUS UPDATE
        # DATABASE OPERATION
        set_job_status_result = await _set_job_status_bulk(
            status_changes,
            config,
            job_db,
            job_logging_db,
            task_queue_db,
            background_task,
            additional_attributes=attribute_changes,
        )

        # BULK JDL UPDATE
        # DATABASE OPERATION
        await job_db.setJobJDLsBulk(jdl_changes)

        return {
            "failed": failed,
            "success": {
                job_id: {
                    "InputData": job_jdls[job_id],
                    **attribute_changes[job_id],
                    **set_status_result.model_dump(),
                }
                for job_id, set_status_result in set_job_status_result.success.items()
            },
        }

    return {
        "success": [],
        "failed": failed,
    }


async def _set_job_status_bulk(
    status_changes: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    *,
    force: bool = False,
    additional_attributes: dict[int, dict[str, str]] = {},
) -> SetJobStatusReturn:
    """Set various status fields for job specified by its jobId.
    Set only the last status in the JobDB, updating all the status
    logging information in the JobLoggingDB. The status dict has datetime
    as a key and status information dictionary as values.

    :raises: JobNotFound if the job is not found in one of the DBs
    """
    from DIRAC.Core.Utilities import TimeUtilities
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Utilities.JobStatusUtility import (
        getNewStatus,
        getStartAndEndTime,
    )

    failed: dict[int, Any] = {}
    deletable_killable_jobs = set()
    job_attribute_updates: dict[int, dict[str, str]] = {}
    job_logging_updates: list[JobLoggingRecord] = []
    status_dicts: dict[int, dict[datetime, dict[str, str]]] = defaultdict(dict)

    # transform JobStateUpdate objects into dicts
    status_dicts = {
        job_id: {
            key: {k: v for k, v in value.model_dump().items() if v is not None}
            for key, value in status.items()
        }
        for job_id, status in status_changes.items()
    }

    # search all jobs at once
    _, results = await job_db.search(
        parameters=["Status", "StartExecTime", "EndExecTime", "JobID"],
        search=[
            {
                "parameter": "JobID",
                "operator": VectorSearchOperator.IN,
                "values": list(set(status_changes.keys())),
            }
        ],
        sorts=[],
    )
    if not results:
        return SetJobStatusReturn(
            success={},
            failed={
                int(job_id): {"detail": "Not found"} for job_id in status_changes.keys()
            },
        )

    found_jobs = set(int(res["JobID"]) for res in results)
    failed.update(
        {
            int(nf_job_id): {"detail": "Not found"}
            for nf_job_id in set(status_changes.keys()) - found_jobs
        }
    )
    # Get the latest time stamps of major status updates
    wms_time_stamps = await job_logging_db.get_wms_time_stamps_bulk(found_jobs)

    for res in results:
        job_id = int(res["JobID"])
        currentStatus = res["Status"]
        startTime = res["StartExecTime"]
        endTime = res["EndExecTime"]

        # If the current status is Stalled and we get an update, it should probably be "Running"
        if currentStatus == JobStatus.STALLED:
            currentStatus = JobStatus.RUNNING

        #####################################################################################################
        statusDict = status_dicts[job_id]
        # This is more precise than "LastTime". timeStamps is a sorted list of tuples...
        timeStamps = sorted((float(t), s) for s, t in wms_time_stamps[job_id].items())
        lastTime = TimeUtilities.fromEpoch(timeStamps[-1][0]).replace(
            tzinfo=timezone.utc
        )

        # Get chronological order of new updates
        updateTimes = sorted(statusDict)

        newStartTime, newEndTime = getStartAndEndTime(
            startTime, endTime, updateTimes, timeStamps, statusDict
        )

        job_data: dict[str, str] = {}
        if updateTimes[-1] >= lastTime:
            new_status, new_minor, new_application = (
                returnValueOrRaise(  # TODO: Catch this
                    getNewStatus(
                        job_id,
                        updateTimes,
                        lastTime,
                        statusDict,
                        currentStatus,
                        force,
                        MagicMock(),  # FIXME
                    )
                )
            )

            if new_status:
                job_data.update(additional_attributes.get(job_id, {}))
                job_data["Status"] = new_status
                job_data["LastUpdateTime"] = str(datetime.now(timezone.utc))
            if new_minor:
                job_data["MinorStatus"] = new_minor
            if new_application:
                job_data["ApplicationStatus"] = new_application

            # TODO: implement elasticJobParametersDB ?
            # if cls.elasticJobParametersDB:
            #     result = cls.elasticJobParametersDB.setJobParameter(int(jobID), "Status", status)
            #     if not result["OK"]:
            #         return result

        for updTime in updateTimes:
            if statusDict[updTime]["Source"].startswith("Job"):
                job_data["HeartBeatTime"] = str(updTime)

        if not startTime and newStartTime:
            job_data["StartExecTime"] = newStartTime

        if not endTime and newEndTime:
            job_data["EndExecTime"] = newEndTime

        #####################################################################################################
        # delete or kill job, if we transition to DELETED or KILLED state
        if new_status in [JobStatus.DELETED, JobStatus.KILLED]:
            deletable_killable_jobs.add(job_id)

        # Update database tables
        if job_data:
            job_attribute_updates[job_id] = job_data

        for updTime in updateTimes:
            sDict = statusDict[updTime]
            job_logging_updates.append(
                JobLoggingRecord(
                    job_id=job_id,
                    status=sDict.get("Status", "idem"),
                    minor_status=sDict.get("MinorStatus", "idem"),
                    application_status=sDict.get("ApplicationStatus", "idem"),
                    date=updTime,
                    source=sDict.get("Source", "Unknown"),
                )
            )

    await job_db.setJobAttributesBulk(job_attribute_updates)

    await _remove_jobs_from_task_queue(
        list(deletable_killable_jobs), config, task_queue_db, background_task
    )

    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID(job_ids))

    if deletable_killable_jobs:
        await job_db.set_job_command_bulk(
            [(job_id, "Kill", "") for job_id in deletable_killable_jobs]
        )

    await job_logging_db.bulk_insert_record(job_logging_updates)

    return SetJobStatusReturn(
        success=job_attribute_updates,
        failed=failed,
    )


async def _remove_jobs_from_task_queue(
    job_ids: list[int],
    config: Config,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
):
    """Remove the job from TaskQueueDB."""
    tq_infos = await task_queue_db.get_tq_infos_for_jobs(job_ids)
    await task_queue_db.remove_jobs(job_ids)
    for tq_id, owner, owner_group, vo in tq_infos:
        # TODO: move to Celery
        background_task.add_task(
            task_queue_db.delete_task_queue_if_empty,
            tq_id,
            owner,
            owner_group,
            config.Registry[vo].Groups[owner_group].JobShare,
            config.Registry[vo].Groups[owner_group].Properties,
            config.Operations[vo].Services.JobScheduling.EnableSharesCorrection,
            config.Registry[vo].Groups[owner_group].AllowBackgroundTQs,
        )


@router.delete("/")
async def remove_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    sandbox_metadata_db: SandboxMetadataDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
):
    """Fully remove a list of jobs from the WMS databases.

    WARNING: This endpoint has been implemented for the compatibility with the legacy DIRAC WMS
    and the JobCleaningAgent. However, once this agent is ported to diracx, this endpoint should
    be removed, and a status change to Deleted (PATCH /jobs/status) should be used instead for any other purpose.
    """
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    # Remove the staging task from the StorageManager
    # TODO: this was not done in the JobManagerHandler, but it was done in the kill method
    # I think it should be done here too
    # TODO: implement StorageManagerClient
    # returnValueOrRaise(StorageManagerClient().killTasksBySourceTaskID([job_id]))

    # TODO: this was also  not done in the JobManagerHandler, but it was done in the JobCleaningAgent
    # I think it should be done here as well
    await sandbox_metadata_db.unassign_sandboxes_to_jobs(job_ids)

    # Remove the job from TaskQueueDB
    await _remove_jobs_from_task_queue(job_ids, config, task_queue_db, background_task)

    # Remove the job from JobLoggingDB
    await job_logging_db.delete_records(job_ids)

    # Remove the job from JobDB
    await job_db.delete_jobs(job_ids)

    return job_ids


@router.patch("/status")
async def set_job_statuses(
    job_update: dict[int, dict[datetime, JobStatusUpdate]],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    force: bool = False,
) -> SetJobStatusReturn:
    await check_permissions(
        action=ActionType.MANAGE, job_db=job_db, job_ids=list(job_update)
    )
    # check that the datetime contains timezone info
    for job_id, status in job_update.items():
        for dt in status:
            if dt.tzinfo is None:
                raise HTTPException(
                    status_code=HTTPStatus.BAD_REQUEST,
                    detail=f"Timestamp {dt} is not timezone aware for job {job_id}",
                )
    result = await _set_job_status_bulk(
        job_update,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        background_task,
        force=force,
    )
    if not result.success:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=result.model_dump(),
        )

    return result


@router.post("/reschedule")
async def reschedule_bulk_jobs(
    job_ids: Annotated[list[int], Query()],
    config: Config,
    job_db: JobDB,
    job_logging_db: JobLoggingDB,
    task_queue_db: TaskQueueDB,
    background_task: BackgroundTasks,
    check_permissions: CheckWMSPolicyCallable,
    reset_jobs: Annotated[bool, Query()] = False,
):
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=job_ids)

    resched_jobs = await _reschedule_jobs_bulk(
        job_ids,
        config,
        job_db,
        job_logging_db,
        task_queue_db,
        background_task,
        reset_counter=reset_jobs,
    )

    if not resched_jobs.get("success", []):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail=resched_jobs,
        )

    # TODO: send jobs to OtimizationMind
    #  self.__sendJobsToOptimizationMind(validJobList)

    return resched_jobs
