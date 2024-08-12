from httpx import Timeout, Limits, AsyncBaseTransport, AsyncClient
from httpx._types import (
    AuthTypes,
    QueryParamTypes,
    HeaderTypes,
    CookieTypes,
    VerifyTypes,
    CertTypes,
    ProxyTypes,
    ProxiesTypes,
    TimeoutTypes,
    URLTypes,
)
import typing


def global_async_client(
    global_name: str,
    *,
    auth: AuthTypes | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    verify: VerifyTypes = True,
    cert: CertTypes | None = None,
    http1: bool = True,
    http2: bool = False,
    proxy: ProxyTypes | None = None,
    proxies: ProxiesTypes | None = None,
    mounts: None | typing.Mapping[str, AsyncBaseTransport | None] = None,
    timeout: TimeoutTypes = Timeout(timeout=5.0),
    follow_redirects: bool = False,
    limits: Limits = Limits(
        max_connections=100, max_keepalive_connections=20, keepalive_expiry=5.0
    ),
    max_redirects: int = 20,
    event_hooks: None
    | typing.Mapping[str, list[typing.Callable[..., typing.Any]]] = None,
    base_url: URLTypes = "",
    transport: AsyncBaseTransport | None = None,
    app: typing.Callable[..., typing.Any] | None = None,
    trust_env: bool = True,
    default_encoding: str | typing.Callable[[bytes], str] = "utf-8",
) -> AsyncClient:
    if global_name not in globals():
        globals()[global_name] = AsyncClient(
            auth=auth,
            params=params,
            headers=headers,
            cookies=cookies,
            verify=verify,
            cert=cert,
            http1=http1,
            http2=http2,
            proxy=proxy,
            proxies=proxies,
            mounts=mounts,
            timeout=timeout,
            follow_redirects=follow_redirects,
            limits=limits,
            max_redirects=max_redirects,
            event_hooks=event_hooks,
            base_url=base_url,
            app=app,
            trust_env=trust_env,
            default_encoding=default_encoding,
        )
    return globals()[global_name]
