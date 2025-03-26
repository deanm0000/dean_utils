from __future__ import annotations

import typing

from httpx import AsyncClient, Limits, Timeout

if typing.TYPE_CHECKING:
    import ssl

    from httpx import AsyncBaseTransport
    from httpx._types import (
        AuthTypes,
        CookieTypes,
        HeaderTypes,
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
    http2: bool = False,
    proxy: ProxyTypes | None = None,
    timeout: TimeoutTypes = _default_timeout,
    limits: Limits = _default_limit,
    max_redirects: int = 20,
    base_url: URLTypes = "",
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
            http2=http2,
            proxy=proxy,
            timeout=timeout,
            limits=limits,
            max_redirects=max_redirects,
            base_url=base_url,
            trust_env=trust_env,
            default_encoding=default_encoding,
        )
    return globals()[global_name]
