"""Task definition to clean Sandboxe Store."""

from __future__ import annotations

import dataclasses

from diracx.logic.jobs.sandboxes import clean_sandboxes
from diracx.tasks.plumbing.base_task import PeriodicBaseTask
from diracx.tasks.plumbing.enums import Priority, Size
from diracx.tasks.plumbing.retry_policies import ExponentialBackoff
from diracx.tasks.plumbing.schedules import CronSchedule

from .depends import SandboxMetadataDB, SandboxStoreSettings


@dataclasses.dataclass
class CleanSandboxStoreTask(PeriodicBaseTask):
    priority = Priority.BACKGROUND
    size = Size.MEDIUM
    retry_policy = ExponentialBackoff(base_delay_seconds=300, max_retries=3)
    default_schedule = CronSchedule("0 6 * * *")

    async def execute(  # type: ignore
        self,
        sandbox_metadata_db: SandboxMetadataDB,
        settings: SandboxStoreSettings,
        **kwargs,
    ) -> int:
        return await clean_sandboxes(sandbox_metadata_db, settings)
