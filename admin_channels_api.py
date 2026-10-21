from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError

from admin_channels_service import find_channel, list_channels, serialize_channel, update_channel
from admin_schemas import ChannelCreateRequest, ChannelOut, ChannelUpdateRequest, Platform
from channel_registration import (RegistrationError, describe_error, register_twitch_channel,
                                  register_youtube_channel)
from models import PLATFORM_TWITCH, SessionLocal
import twitch_eventsub

router = APIRouter()


@router.get("/channels", response_model=list[ChannelOut])
def get_channels():
    db = SessionLocal()
    try:
        return list_channels(db)
    finally:
        db.close()


@router.post("/channels", status_code=201)
def create_channel(body: ChannelCreateRequest):
    register = register_twitch_channel if body.platform == PLATFORM_TWITCH else register_youtube_channel
    guild_id = int(body.discord_guild_id) if body.discord_guild_id else None
    channel_id = int(body.discord_channel_id) if body.discord_channel_id else None
    try:
        result = register(body.identifier, guild_id, channel_id, body.custom_message)
    except RegistrationError as e:
        raise HTTPException(status_code=e.http_status, detail=e.message)
    except SQLAlchemyError:
        raise HTTPException(status_code=500, detail="DB操作に失敗しました。時間をおいて再度お試しください。")

    if not result.created_channel and result.destination in (None, "unchanged"):
        raise HTTPException(status_code=409, detail="このチャンネルは既に登録済みです（通知先の追加・変更もありません）。")
    db = SessionLocal()
    try:
        channel = serialize_channel(find_channel(db, result.platform, result.channel_id))
    finally:
        db.close()
    return {
        "channel": channel,
        "created_channel": result.created_channel,
        "destination": result.destination,
        "subscription": {
            "status": result.subscription_status,
            "error": result.subscription_error,
            "message": describe_error(result.subscription_error),
        },
    }


@router.patch("/channels/{channel_id}")
def patch_channel(channel_id: str, body: ChannelUpdateRequest, platform: Platform = Query("youtube")):
    db = SessionLocal()
    try:
        channel = update_channel(db, platform, channel_id, body.is_active, body.name)
    finally:
        db.close()

    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel


@router.post("/channels/{channel_id}/resubscribe")
def resubscribe_channel(channel_id: str, platform: Platform = Query("twitch")):
    """Twitchの購読を作り直す(失効・失敗時やWebhookシークレット変更後の手動復旧用)。"""
    if platform != PLATFORM_TWITCH:
        raise HTTPException(status_code=400, detail="再購読はTwitchチャンネルのみ対応しています（YouTubeは毎時自動更新）。")
    db = SessionLocal()
    try:
        broadcaster = find_channel(db, platform, channel_id)
        broadcaster_pk = broadcaster.id if broadcaster else None
    finally:
        db.close()
    if broadcaster_pk is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    result = twitch_eventsub.ensure_subscription(broadcaster_pk, force=True, recreate=True)
    if result.status == twitch_eventsub.STATUS_ERROR:
        raise HTTPException(status_code=502, detail="購読の作成に失敗しました: " + describe_error(result.error))
    return {"status": result.status}
