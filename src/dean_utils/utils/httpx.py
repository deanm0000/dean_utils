from __future__ import annotations

import typing

from httpx import AsyncClient, Limits, Timeout

if typing.TYPE_CHECKING:
    import ssl

    from httpx import AsyncBaseTransport
    from httpx._types import (
        AuthTypes,
        CertTypes,
        CookieTypes,
        HeaderTypes,
        ProxiesTypes,
        ProxyTypes,
        QueryParamTypes,
        TimeoutTypes,
        URLTypes,
    )
_default_limit = Limits(
    max_connections=100, max_keepalive_connections=20, keepalive_expiry=5.0
)
_default_timeout = Timeout(timeout=5.0)


def global_async_client(
    global_name: str,
    *,
    auth: AuthTypes | None = None,
    params: QueryParamTypes | None = None,
    headers: HeaderTypes | None = None,
    cookies: CookieTypes | None = None,
    verify: ssl.SSLContext | str | bool = True,
    cert: CertTypes | None = None,
    http1: bool = True,
    http2: bool = False,
    proxy: ProxyTypes | None = None,
    mounts: None | typing.Mapping[str, AsyncBaseTransport | None] = None,
    timeout: TimeoutTypes = _default_timeout,
    follow_redirects: bool = False,
    limits: Limits = _default_limit,
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
