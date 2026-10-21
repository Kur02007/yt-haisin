"""Twitch配信開始通知と EventSub 購読管理の Celery タスク。

YouTube側(tasks.py / subscription_tasks.py)とはテーブル行を platform で分けており、
このモジュールは platform='twitch' の行しか読み書きしない。
"""
import logging
import time
from datetime import datetime, timedelta

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from celery_app import celery_app
from config import Config
from i18n import DEFAULT_LANGUAGE, get_guild_languages, t
from discord_notifier import (EMBED_STYLES, SEND_CHANNEL_MISSING, SEND_OK, edit_message_embed,
                              send_stream_notification)
from models import (PLATFORM_TWITCH, Broadcaster, LiveStream, SessionLocal, Subscription,
                    TwitchStreamNotification)
import tasks as youtube_tasks
import twitch_api
import twitch_eventsub
from twitch_api import TwitchAPIError

logger = logging.getLogger(__name__)

# live_streams.video_id を配信IDの重複排除に共用する。YouTubeの動画IDと衝突しないよう接頭辞を付ける。
STREAM_KEY_PREFIX = "twitch:"
# 失効(notification_failures_exceeded等)直後に即再購読すると同じ原因で再び失敗しやすいため少し待つ。
REVOCATION_RESUBSCRIBE_DELAY_SECONDS = 300
TWITCH_URL_TEMPLATE = "https://www.twitch.tv/{login}"
# Twitchの配信プレビュー画像。Discordが古い画像をキャッシュしないよう時刻を付ける。
PREVIEW_URL_TEMPLATE = "https://static-cdn.jtvnw.net/previews-ttv/live_user_{login}-1280x720.jpg?t={ts}"
# 既存の通知文言(build_default_content_text)の「🔴 **名前** が配信を開始しました！」をそのまま使う。
LIVE_CLASSIFICATION = {"broadcast_state": "live", "scheduled_time_jst": None, "is_members_only": False}
# 配信終了後、アーカイブ(VOD)が見つかるまでの再確認間隔(秒)。通常は1回目で見つかる。
ARCHIVE_RETRY_DELAYS = (120, 600, 1800)
# アーカイブ差し替えのために保持する通知記録の日数。
NOTIFICATION_RETENTION_DAYS = 30
# タイトルが取れなかったときの仮タイトル(DBにはこの値で残す)。通知では各サーバーの言語に置き換える。
FALLBACK_TITLE = "配信"


def stream_key(stream_id):
    return STREAM_KEY_PREFIX + str(stream_id)


def _fetch_title(broadcaster_user_id):
    # stream.online イベント自体にはタイトルが無いため Get Channel Information で補う。
    try:
        info = twitch_api.get_channel_info(broadcaster_user_id) or {}
        return info.get("title") or FALLBACK_TITLE
    except TwitchAPIError as error:
        logger.warning("[twitch_api] channel title unavailable code=%s; using fallback", error.code)
        return FALLBACK_TITLE


def build_stream_data(event, title):
    login = event.get("broadcaster_user_login") or ""
    return {
        "channel_id": event["broadcaster_user_id"],
        "video_id": stream_key(event["id"]),
        "title": title,
        "link": TWITCH_URL_TEMPLATE.format(login=login),
        "published": event.get("started_at"),
        "thumbnail_url": PREVIEW_URL_TEMPLATE.format(login=login, ts=int(time.time())),
        "author_name": event.get("broadcaster_user_name") or login or "Unknown",
    }


@celery_app.task(bind=True, max_retries=3)
def process_twitch_stream_online_task(self, event):
    """stream.online 1件分の通知。EventSubのメッセージID重複は main.py で、
    同一配信(stream id)の重複はここで live_streams を使って排除する。"""
    db: Session = SessionLocal()
    try:
        broadcaster_user_id = str(event["broadcaster_user_id"])
        key = stream_key(event["id"])

        broadcaster = db.query(Broadcaster).filter(
            Broadcaster.platform == PLATFORM_TWITCH,
            Broadcaster.channel_id == broadcaster_user_id,
        ).first()
        if broadcaster is None:
            return "Unknown broadcaster"
        # YouTubeと同じく、dedup登録より前に判定して再有効化後の配信を取りこぼさない。
        if broadcaster.is_active is False:
            return "Inactive channel, skipped"
        if db.query(LiveStream).filter(LiveStream.video_id == key).first():
            return "Duplicate"

        title = _fetch_title(broadcaster_user_id)
        db.add(LiveStream(video_id=key, title=title, published_at=datetime.utcnow()))
        # ログイン名・表示名は配信者が変えられるため、最新値に追従する。
        login = event.get("broadcaster_user_login")
        if login and broadcaster.twitch_login != login:
            broadcaster.twitch_login = login
        display_name = event.get("broadcaster_user_name")
        if display_name and broadcaster.name != display_name:
            broadcaster.name = display_name
        try:
            db.commit()
        except IntegrityError:
            # 同じ配信の通知を別ワーカーが先に登録した
            db.rollback()
            return "Duplicate"

        subs = db.query(Subscription).filter(
            Subscription.platform == PLATFORM_TWITCH,
            Subscription.youtube_channel_id == broadcaster_user_id,
        ).all()
        if not subs:
            return "No subscribers"

        data = build_stream_data(event, title)
        # 通知先サーバーの言語設定(/language)。失敗しても ja で送るだけ(tasks.py と同じ理由)。
        guild_languages = get_guild_languages(sub.discord_guild_id for sub in subs)
        sent_count = 0
        removed_count = 0
        for sub in subs:
            language = guild_languages.get(sub.discord_guild_id, DEFAULT_LANGUAGE)
            sub_data = data
            if title == FALLBACK_TITLE and language != DEFAULT_LANGUAGE:
                sub_data = dict(data, title=t(language, "kind_live"))
            result, message_id = send_stream_notification(
                sub.discord_channel_id, sub_data, LIVE_CLASSIFICATION, sub.custom_message,
                platform=PLATFORM_TWITCH, language=language)
            if result == SEND_OK:
                sent_count += 1
                if sub.fail_count:
                    sub.fail_count = 0
                if message_id is not None:
                    # 配信終了後にリンクをアーカイブへ差し替えるため、送り先と本文IDを控える
                    db.add(TwitchStreamNotification(
                        stream_id=str(event["id"]), broadcaster_user_id=broadcaster_user_id,
                        discord_channel_id=sub.discord_channel_id, discord_message_id=message_id))
            elif result == SEND_CHANNEL_MISSING:
                if youtube_tasks._count_channel_missing(sub):
                    db.delete(sub)
                    removed_count += 1
            time.sleep(0.1)
        db.commit()

        if removed_count:
            return f"Sent {sent_count} notifications (removed {removed_count} dead subscriptions)"
        return f"Sent {sent_count} notifications"

    except SQLAlchemyError as e:
        logger.error(f"[system_error:db] Twitch通知のDB操作に失敗しました (stream={event.get('id')}): {e}")
        raise self.retry(exc=e, countdown=60)
    except Exception as e:
        logger.error(f"[system_error:unknown] Twitch通知で予期しない内部エラー (stream={event.get('id')}): {e}")
        raise self.retry(exc=e, countdown=60)
    finally:
        db.close()


@celery_app.task(ignore_result=True)
def ensure_twitch_subscription_task(broadcaster_pk, force=False):
    return tuple(twitch_eventsub.ensure_subscription(broadcaster_pk, force=force))


@celery_app.task(ignore_result=True)
def handle_twitch_revocation_task(subscription):
    broadcaster_pk = twitch_eventsub.record_revocation(subscription)
    if broadcaster_pk is not None:
        ensure_twitch_subscription_task.apply_async(args=[broadcaster_pk],
                                                    countdown=REVOCATION_RESUBSCRIBE_DELAY_SECONDS)


@celery_app.task(ignore_result=True)
def reconcile_twitch_subscriptions():
    """起動時と毎時: 有効なTwitchチャンネル全件の購読が存在することを確認し、失効していれば作り直す。"""
    error = Config.twitch_config_error()
    with SessionLocal() as db:
        twitch_eventsub.prune_messages(db)
        (db.query(TwitchStreamNotification)
         .filter(TwitchStreamNotification.created_at
                 < twitch_eventsub.utcnow() - timedelta(days=NOTIFICATION_RETENTION_DAYS))
         .delete(synchronize_session=False))
        db.commit()
        ids = [row[0] for row in db.query(Broadcaster.id).filter(
            Broadcaster.platform == PLATFORM_TWITCH, Broadcaster.is_active.is_not(False)).all()]
    if not ids:
        return {}
    if error:
        logger.error("Twitch EventSub reconcile skipped code=%s channels=%d", error, len(ids))
        return {"skipped": len(ids)}
    counts = {}
    for broadcaster_pk in ids:
        status = twitch_eventsub.ensure_subscription(broadcaster_pk).status or "missing"
        counts[status] = counts.get(status, 0) + 1
        time.sleep(0.2)
    level = logging.INFO if set(counts) <= set(twitch_eventsub.ACTIVE_STATUSES) else logging.WARNING
    logger.log(level, "Twitch EventSub summary " + " ".join("%s=%d" % kv for kv in sorted(counts.items())))
    return counts


# --- 配信終了 → 通知のリンクをアーカイブ(VOD)へ差し替える ---------------------------------

def build_archive_embed(video, title, author_name, language=DEFAULT_LANGUAGE):
    """終了後の埋め込み。リンクをアーカイブのURLに向け、サムネイルもVODのものにする。
    language は通知先サーバーの言語設定(i18n.py)。"""
    thumbnail = (video.get("thumbnail_url") or "").replace("%{width}", "1280").replace("%{height}", "720")
    if not title or title == FALLBACK_TITLE:
        title = t(language, "kind_live")
    return {
        "title": t(language, "archive_title_prefix") + title,
        "url": video.get("url") or TWITCH_URL_TEMPLATE.format(login=""),
        "color": EMBED_STYLES[PLATFORM_TWITCH]["color"],
        "image": {"url": thumbnail} if thumbnail else {},
        "author": {"name": author_name},
        "fields": [{"name": t(language, "archive_duration"), "value": video.get("duration") or "-", "inline": True}],
        "footer": {"text": EMBED_STYLES[PLATFORM_TWITCH]["footer"] + t(language, "archive_ended")},
        "timestamp": video.get("created_at"),
    }


@celery_app.task(bind=True, max_retries=3)
def process_twitch_stream_offline_task(self, event, attempt=0):
    """配信終了。その配信の通知メッセージを探し、リンクをアーカイブ(VOD)へ差し替える。

    アーカイブは終了直後にはまだ現れないことがあるため、見つからなければ時間をおいて数回試す。
    配信者が「過去の配信を保存」をオフにしている場合はアーカイブが存在しないので、
    一定回数で諦めて元の通知をそのまま残す(通知を消したり増やしたりはしない)。
    """
    broadcaster_user_id = str(event["broadcaster_user_id"])
    with SessionLocal() as db:
        rows = (db.query(TwitchStreamNotification)
                .filter(TwitchStreamNotification.broadcaster_user_id == broadcaster_user_id,
                        TwitchStreamNotification.archived.is_(False))
                .order_by(TwitchStreamNotification.id.desc()).all())
        if not rows:
            return "No notification to update"
        # 直近の配信(=最後に通知した stream_id)だけを対象にする
        stream_id = rows[0].stream_id
        targets = [(row.id, row.discord_channel_id, row.discord_message_id)
                   for row in rows if row.stream_id == stream_id]
        stream = db.query(LiveStream).filter(LiveStream.video_id == stream_key(stream_id)).first()
        title = stream.title if stream else None
        broadcaster = db.query(Broadcaster).filter(
            Broadcaster.platform == PLATFORM_TWITCH,
            Broadcaster.channel_id == broadcaster_user_id).first()
        author_name = (event.get("broadcaster_user_name")
                       or (broadcaster.name if broadcaster else None) or "Unknown")
        # 通知記録にはサーバーIDが無いので、送り先チャンネルの登録からサーバーを割り出す。
        # 登録が削除済みなら見つからず ja になる。
        channel_guilds = dict(db.query(Subscription.discord_channel_id, Subscription.discord_guild_id).filter(
            Subscription.platform == PLATFORM_TWITCH,
            Subscription.youtube_channel_id == broadcaster_user_id,
            Subscription.discord_channel_id.in_([channel_id for _, channel_id, _ in targets])).all())

    try:
        video = twitch_api.get_archive_video(broadcaster_user_id, stream_id)
    except TwitchAPIError as error:
        logger.warning("[twitch_archive] アーカイブ取得に失敗 code=%s stream=%s", error.code, stream_id)
        video = None
    if video is None:
        if attempt < len(ARCHIVE_RETRY_DELAYS):
            process_twitch_stream_offline_task.apply_async(
                args=[event, attempt + 1], countdown=ARCHIVE_RETRY_DELAYS[attempt])
            return "Archive not ready, retrying"
        # 保存設定オフ等。元の通知はそのまま残し、再確認もここで打ち切る。
        logger.info("[twitch_archive] アーカイブが見つかりませんでした(保存設定オフの可能性) stream=%s", stream_id)
        _mark_archived([row_id for row_id, _, _ in targets])
        return "Archive unavailable"

    guild_languages = get_guild_languages(channel_guilds.values())
    embeds = {}  # 言語ごとに1回だけ組み立てる
    updated = 0
    done = []
    for row_id, channel_id, message_id in targets:
        language = guild_languages.get(channel_guilds.get(channel_id), DEFAULT_LANGUAGE)
        if language not in embeds:
            embeds[language] = build_archive_embed(video, title, author_name, language)
        result = edit_message_embed(channel_id, message_id, embeds[language])
        if result == SEND_OK:
            updated += 1
        # 成功・対象なしはこれ以上試さない。一時的な失敗だけ記録を残し、次の機会に再試行する。
        if result in (SEND_OK, SEND_CHANNEL_MISSING):
            done.append(row_id)
        time.sleep(0.1)
    _mark_archived(done)
    logger.info("[twitch_archive] アーカイブへ差し替えました stream=%s updated=%d/%d",
                stream_id, updated, len(targets))
    return f"Updated {updated}/{len(targets)} notifications"


def _mark_archived(row_ids):
    if not row_ids:
        return
    with SessionLocal() as db:
        (db.query(TwitchStreamNotification)
         .filter(TwitchStreamNotification.id.in_(row_ids))
         .update({TwitchStreamNotification.archived: True}, synchronize_session=False))
        db.commit()
