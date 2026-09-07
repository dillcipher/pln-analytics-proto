"""Compatibility shim for legacy Google Drive recovery hooks.

Automatic Drive recovery is intentionally disabled at startup because the API
runtime uses ephemeral disk for large raw workbooks. Recovery decisions belong
to explicit user actions, not a background wrapper around every new sync.

This module remains import-compatible with older deployments, but it must never
intercept or rewrite a freshly created /drive/sync worker.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
_INSTALLED = False


def install_drive_resume_patch() -> None:
    """Keep legacy startup imports safe without monkey-patching Drive workers."""
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    logger.info(
        "Drive resume compatibility shim active: fresh Drive sync workers are not intercepted."
    )
