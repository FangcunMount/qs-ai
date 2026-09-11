"""Stable ASGI factory import; implementation lives in the composition root."""

from qs_ai.bootstrap.api import create_app

__all__ = ["create_app"]
