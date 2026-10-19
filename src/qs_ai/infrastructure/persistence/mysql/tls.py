"""One verified MySQL TLS policy for services and maintenance processes."""

import os
import ssl
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def tls_context(mode: str, ca_file: str | None) -> ssl.SSLContext | None:
    mode = mode.strip().upper()
    if mode in ("", "DISABLED"):
        return None
    if mode != "VERIFY_IDENTITY":
        raise ValueError("Unsupported MySQL TLS mode")
    if not ca_file:
        raise ValueError("MySQL VERIFY_IDENTITY requires a CA PEM file")
    context = ssl.create_default_context(cafile=ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


def create_mysql_engine(
    url: str,
    *,
    tls_mode: str | None = None,
    tls_ca_file: str | None = None,
    **kwargs: Any,
) -> AsyncEngine:
    context = tls_context(
        tls_mode if tls_mode is not None else os.environ.get("QS_AI_DATABASE_TLS_MODE", "DISABLED"),
        tls_ca_file if tls_ca_file is not None else os.environ.get("QS_AI_DATABASE_TLS_CA_FILE"),
    )
    if context is not None:
        if not url.startswith("mysql+asyncmy://"):
            raise ValueError("Verified MySQL TLS requires the asyncmy driver")
        connect_args = dict(kwargs.pop("connect_args", {}))
        connect_args["ssl"] = context
        kwargs["connect_args"] = connect_args
    engine = create_async_engine(url, **kwargs)
    if context is not None:

        @event.listens_for(engine.sync_engine, "connect")
        def require_encrypted_session(connection: Any, _record: Any) -> None:
            cursor = connection.cursor()
            try:
                cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                row = cursor.fetchone()
                if not row or not row[1]:
                    raise RuntimeError("MySQL TLS session was not established")
            finally:
                cursor.close()

    return engine
