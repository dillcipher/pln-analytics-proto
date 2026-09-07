from __future__ import annotations

from fastapi import APIRouter, Depends

from app.interface.api.deps import get_current_user
from app.interface.api.v1 import (
    auth,
    batch_upload,
    dashboard,
    data_management,
    datasets,
    dlpd,
    download,
    drive,
    executive,
    history,
    jobs,
    process,
    suspect,
    upload,
    warehouse,
)

"""
API v1 Router.

All API endpoints are registered here.

SECURITY: every router below except auth.router requires a valid session
(get_current_user, gated by AUTH_REQUIRED -- see app/core/config.py).
Confirmed live 2026-08-31: individual endpoint modules never declared
Depends(get_current_user) themselves, so simply flipping AUTH_REQUIRED to
default true would NOT have protected anything by itself -- this
dependency, applied once here at include_router() (FastAPI applies it to
every route in that router), is what actually enforces it. auth.router
stays open since it IS the login endpoint; requiring a token to fetch a
token would lock everyone out.
"""

api_v1_router = APIRouter(prefix="/api/v1")

api_v1_router.include_router(auth.router)

_authenticated = [Depends(get_current_user)]

# Upload & ETL
api_v1_router.include_router(upload.router, dependencies=_authenticated)
api_v1_router.include_router(batch_upload.router, dependencies=_authenticated)
api_v1_router.include_router(process.router, dependencies=_authenticated)
api_v1_router.include_router(drive.router, dependencies=_authenticated)
api_v1_router.include_router(jobs.router, dependencies=_authenticated)

# Warehouse
api_v1_router.include_router(warehouse.router, dependencies=_authenticated)

# Dataset Management
api_v1_router.include_router(datasets.router, dependencies=_authenticated)
api_v1_router.include_router(data_management.router, dependencies=_authenticated)
api_v1_router.include_router(history.router, dependencies=_authenticated)
api_v1_router.include_router(download.router, dependencies=_authenticated)

# Dashboard
api_v1_router.include_router(dashboard.router, dependencies=_authenticated)
api_v1_router.include_router(executive.router, dependencies=_authenticated)
api_v1_router.include_router(dlpd.router, dependencies=_authenticated)
api_v1_router.include_router(suspect.router, dependencies=_authenticated)
