from fastapi import APIRouter, Depends

from admin_auth import require_admin_api_key
from admin_channels_api import router as channels_router
from admin_schedule_api import router as schedule_router

router = APIRouter(
    prefix="/admin/api",
    dependencies=[Depends(require_admin_api_key)],
)
router.include_router(channels_router)
router.include_router(schedule_router)
