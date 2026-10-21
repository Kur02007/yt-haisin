from models import AppSetting, SessionLocal

LIVE_POLLING_ENABLED_KEY = "live_polling_enabled"


def _get(key: str, default: str) -> str:
    db = SessionLocal()
    try:
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        return row.value if row else default
    finally:
        db.close()


def _set(key: str, value: str) -> None:
    db = SessionLocal()
    try:
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        if row:
            row.value = value
        else:
            db.add(AppSetting(key=key, value=value))
        db.commit()
    finally:
        db.close()


def is_live_polling_enabled() -> bool:
    return _get(LIVE_POLLING_ENABLED_KEY, "true") == "true"


def set_live_polling_enabled(enabled: bool) -> None:
    _set(LIVE_POLLING_ENABLED_KEY, "true" if enabled else "false")
