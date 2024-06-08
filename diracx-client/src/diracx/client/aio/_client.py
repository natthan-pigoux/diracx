# coding=utf-8
# --------------------------------------------------------------------------
# Code generated by Microsoft (R) AutoRest Code Generator (autorest: 3.10.2, generator: @autorest/python@6.13.19)
# Changes may cause incorrect behavior and will be lost if the code is regenerated.
# --------------------------------------------------------------------------

from copy import deepcopy
from typing import Any, Awaitable

from azure.core import AsyncPipelineClient
from azure.core.pipeline import policies
from azure.core.rest import AsyncHttpResponse, HttpRequest

from .. import models as _models
from .._serialization import Deserializer, Serializer
from ._configuration import DiracConfiguration
from .operations import (
    AuthOperations,
    ConfigOperations,
    JobsOperations,
    WellKnownOperations,
)


class Dirac:  # pylint: disable=client-accepts-api-version-keyword
    """Dirac.

    :ivar well_known: WellKnownOperations operations
    :vartype well_known: client.aio.operations.WellKnownOperations
    :ivar auth: AuthOperations operations
    :vartype auth: client.aio.operations.AuthOperations
    :ivar config: ConfigOperations operations
    :vartype config: client.aio.operations.ConfigOperations
    :ivar jobs: JobsOperations operations
    :vartype jobs: client.aio.operations.JobsOperations
    :keyword endpoint: Service URL. Required. Default value is "".
    :paramtype endpoint: str
    """

    def __init__(  # pylint: disable=missing-client-constructor-parameter-credential
        self, *, endpoint: str = "", **kwargs: Any
    ) -> None:
        self._config = DiracConfiguration(**kwargs)
        _policies = kwargs.pop("policies", None)
        if _policies is None:
            _policies = [
                policies.RequestIdPolicy(**kwargs),
                self._config.headers_policy,
                self._config.user_agent_policy,
                self._config.proxy_policy,
                policies.ContentDecodePolicy(**kwargs),
                self._config.redirect_policy,
                self._config.retry_policy,
                self._config.authentication_policy,
                self._config.custom_hook_policy,
                self._config.logging_policy,
                policies.DistributedTracingPolicy(**kwargs),
                (
                    policies.SensitiveHeaderCleanupPolicy(**kwargs)
                    if self._config.redirect_policy
                    else None
                ),
                self._config.http_logging_policy,
            ]
        self._client: AsyncPipelineClient = AsyncPipelineClient(
            base_url=endpoint, policies=_policies, **kwargs
        )

        client_models = {
            k: v for k, v in _models.__dict__.items() if isinstance(v, type)
        }
        self._serialize = Serializer(client_models)
        self._deserialize = Deserializer(client_models)
        self._serialize.client_side_validation = False
        self.well_known = WellKnownOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.auth = AuthOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.config = ConfigOperations(
            self._client, self._config, self._serialize, self._deserialize
        )
        self.jobs = JobsOperations(
            self._client, self._config, self._serialize, self._deserialize
        )

    def send_request(
        self, request: HttpRequest, *, stream: bool = False, **kwargs: Any
    ) -> Awaitable[AsyncHttpResponse]:
        """Runs the network request through the client's chained policies.

        >>> from azure.core.rest import HttpRequest
        >>> request = HttpRequest("GET", "https://www.example.org/")
        <HttpRequest [GET], url: 'https://www.example.org/'>
        >>> response = await client.send_request(request)
        <AsyncHttpResponse: 200 OK>

        For more information on this code flow, see https://aka.ms/azsdk/dpcodegen/python/send_request

        :param request: The network request you want to make. Required.
        :type request: ~azure.core.rest.HttpRequest
        :keyword bool stream: Whether the response payload will be streamed. Defaults to False.
        :return: The response of your network call. Does not do error handling on your response.
        :rtype: ~azure.core.rest.AsyncHttpResponse
        """

        request_copy = deepcopy(request)
        request_copy.url = self._client.format_url(request_copy.url)
        return self._client.send_request(request_copy, stream=stream, **kwargs)  # type: ignore

    async def close(self) -> None:
        await self._client.close()

    async def __aenter__(self) -> "Dirac":
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc_details: Any) -> None:
        await self._client.__aexit__(*exc_details)
