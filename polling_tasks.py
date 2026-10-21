import logging
import random
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from celery_app import celery_app
from config import Config
from live_poller import get_current_live_video_id
from models import PLATFORM_YOUTUBE, Broadcaster, LiveStream, SessionLocal
from settings_store import is_live_polling_enabled
from tasks import process_livestream_task
from thumbnails import get_best_thumbnail
from youtube_api import fetch_video_details

logger = logging.getLogger(__name__)


@celery_app.task
def poll_all_channels():
    """登録済み全チャンネル分のライブ判定を、チャンネルごとの軽量タスクに振り分ける。
    ここ自体はHTTPアクセスを行わないため、beatスケジューラをブロックしない。
    管理画面のトグルはこのDBフラグを介して反映されるため、コンテナ再起動は不要。"""
    if not is_live_polling_enabled():
        return "disabled"

    db: Session = SessionLocal()
    try:
        channel_ids = [
            row[0]
            for row in db.query(Broadcaster.channel_id)
            .filter(Broadcaster.platform == PLATFORM_YOUTUBE, Broadcaster.is_active.is_not(False))
            .all()
        ]
    finally:
        db.close()

    for channel_id in channel_ids:
        # 全チャンネルへ同時にアクセスして負荷が集中/ブロックされないよう、
        # ポーリング間隔の範囲内でランダムにずらして発行する。
        jitter = random.uniform(0, Config.LIVE_POLL_INTERVAL_SECONDS * 0.8)
        poll_channel_live_task.apply_async(args=[channel_id], countdown=jitter)


@celery_app.task(bind=True, max_retries=1)
def poll_channel_live_task(self, channel_id):
    """1チャンネル分のライブ状態チェック。新規のライブを見つけたら通知パイプラインへ渡す。"""
    video_id = get_current_live_video_id(channel_id)
    if not video_id:
        return "not_live"

    db: Session = SessionLocal()
    try:
        already_known = db.query(LiveStream).filter(LiveStream.video_id == video_id).first()
    finally:
        db.close()

    if already_known:
        # process_livestream_task側でも重複判定するが、既知のライブへ毎回タスクを
        # 積まないよう、ここで早めに弾く。
        return "duplicate"

    api_details = fetch_video_details(video_id) or {}
    snippet = api_details.get("snippet", {}) or {}
    if snippet:
        # /liveページの判定は非公式なので、APIで「このチャンネルの」「配信中の」動画かを確かめる。
        # 他チャンネルの動画を通すと、誤通知に加えて登録名まで上書きされてしまう。
        if snippet.get("channelId") != channel_id:
            logger.warning(
                f"[live_poller] channel_id={channel_id} で別チャンネルの動画を検出したため破棄: "
                f"video_id={video_id} owner={snippet.get('channelId')}"
            )
            return "foreign_video"
        if snippet.get("liveBroadcastContent") != "live":
            return "not_live"

    video_data = _build_video_data(channel_id, video_id, api_details)
    process_livestream_task.delay(video_data)
    return "dispatched"


def _build_video_data(channel_id, video_id, api_details):
    """PubSubHubbub通知と同じ形のvideo_dataを、videos.list(1クオータ)の結果で組み立てる。"""
    snippet = api_details.get("snippet", {}) or {}

    return {
        "channel_id": channel_id,
        "video_id": video_id,
        "title": snippet.get("title", "配信"),
        "link": f"https://www.youtube.com/watch?v={video_id}",
        "published": snippet.get("publishedAt", datetime.now(timezone.utc).isoformat()),
        "thumbnail_url": get_best_thumbnail(video_id),
        "author_name": snippet.get("channelTitle", "Unknown"),
    }
