from typing import Optional

from sqlalchemy.orm import Session

from models import PLATFORM_TWITCH, Broadcaster


def _channel_url(b) -> Optional[str]:
    if b.platform == PLATFORM_TWITCH:
        return f"https://www.twitch.tv/{b.twitch_login}" if b.twitch_login else None
    return f"https://www.youtube.com/channel/{b.channel_id}"


def serialize_channel(b) -> dict:
    twitch = b.platform == PLATFORM_TWITCH
    return {
        "platform": b.platform,
        "channel_id": b.channel_id,
        "name": b.name,
        "is_active": bool(b.is_active) if b.is_active is not None else True,
        "subscriber_count": len(b.subscriptions),
        "last_subscribed_at": b.last_subscribed_at,
        "url": _channel_url(b),
        "twitch_login": b.twitch_login,
        "subscription_status": b.twitch_subscription_status if twitch else b.subscription_state,
        "subscription_error": b.twitch_subscription_error if twitch else b.subscription_last_error,
        # Twitchのみ: 配信終了(stream.offline)の購読。アーカイブへのリンク差し替えに使う。
        "offline_subscription_status": b.twitch_offline_subscription_status if twitch else None,
    }


def list_channels(db: Session) -> list[dict]:
    broadcasters = db.query(Broadcaster).order_by(Broadcaster.platform.desc(), Broadcaster.id).all()
    return [serialize_channel(b) for b in broadcasters]


def find_channel(db: Session, platform: str, channel_id: str):
    return db.query(Broadcaster).filter(
        Broadcaster.platform == platform, Broadcaster.channel_id == channel_id
    ).first()


def update_channel(db: Session, platform: str, channel_id: str,
                   is_active: Optional[bool] = None, name: Optional[str] = None) -> Optional[dict]:
    """成功したら更新後のチャンネル、対象が存在しなければNoneを返す。"""
    broadcaster = find_channel(db, platform, channel_id)
    if not broadcaster:
        return None
    if is_active is not None:
        broadcaster.is_active = is_active
    if name is not None and name.strip():
        broadcaster.name = name.strip()
    db.commit()
    return serialize_channel(broadcaster)


def set_channel_active(db: Session, channel_id: str, is_active: bool, platform: str = "youtube") -> bool:
    """成功したらTrue、対象チャンネルが存在しなければFalseを返す。"""
    return update_channel(db, platform, channel_id, is_active=is_active) is not None
