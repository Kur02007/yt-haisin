from fastapi import FastAPI, Request, Response, Header, BackgroundTasks
from starlette.concurrency import run_in_threadpool
import json
import logging
import re

from config import Config
from tasks import process_livestream_task
from models import PLATFORM_YOUTUBE, init_db, SessionLocal, Broadcaster
from subscription_tasks import denial_reason, record_subscription_denial, topic_url
from webhook_security import verify_signature
from feed_logger import append_raw_feed_log, append_structured_feed_log
from feed_parser import parse_video_entry, FeedParseError
import twitch_eventsub
from twitch_tasks import (handle_twitch_revocation_task, process_twitch_stream_offline_task,
                          process_twitch_stream_online_task, reconcile_twitch_subscriptions)

# ログの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebhookAccessLogFilter(logging.Filter):
    """Uvicorn normally logs verification query strings, including topic/challenge."""
    def filter(self, record):
        if isinstance(record.args, tuple) and len(record.args) == 5:
            args = list(record.args)
            if str(args[2]).split("?", 1)[0] == "/webhook/youtube":
                args[2] = "/webhook/youtube"
                record.args = tuple(args)
        return True


logging.getLogger("uvicorn.access").addFilter(WebhookAccessLogFilter())

# ※管理ダッシュボード(/admin/api, manager.html)はこのアプリには含めない。
# こちらはYouTubeからの通知を受けるため常時インターネット公開が前提だが、
# 現状TLSが無い(平文HTTP)ため、認証キーを平文でやり取りする管理系は
# admin_app.py として別プロセス/別ポート(127.0.0.1限定公開)に分離している。
app = FastAPI()


# 起動時にDBテーブルを確認
@app.on_event("startup")
def on_startup():
    init_db()
    # 起動時にTwitch購読の存在を確認する。Twitchの検証要求にこのプロセスが
    # 応答できるよう、受信開始後に実行されるだけの猶予を置く。
    if Config.twitch_enabled():
        try:
            reconcile_twitch_subscriptions.apply_async(countdown=30)
        except Exception:
            logger.warning("Twitch EventSub startup check enqueue failed count=1; hourly check will recover")


@app.get("/webhook/youtube")
def verify_subscription(request: Request):
    """Validate exact registered topics; callback alone never confirms a new lease."""
    params = request.query_params
    mode = params.get("hub.mode")
    topic = params.get("hub.topic")
    challenge = params.get("hub.challenge")
    headers = {"X-Content-Type-Options": "nosniff"}
    reject = lambda: Response(status_code=404, headers=headers)
    if mode not in ("subscribe", "unsubscribe", "denied") or not topic:
        return reject()
    if any(len(params.getlist(key)) != 1 for key in ("hub.mode", "hub.topic")):
        return reject()
    match = re.fullmatch(r"https://www\.youtube\.com/xml/feeds/videos\.xml\?channel_id=(UC[A-Za-z0-9_-]{22})", topic)
    if not match or topic != topic_url(match.group(1)):
        return reject()
    if mode != "denied" and (challenge is None or len(params.getlist("hub.challenge")) != 1):
        return reject()
    try:
        with SessionLocal() as db:
            b = db.query(Broadcaster).filter(Broadcaster.platform == PLATFORM_YOUTUBE,
                                             Broadcaster.channel_id == match.group(1)).first()
            if b is None or (mode == "subscribe" and b.is_active is False):
                return reject()
            if mode == "denied":
                # Only sanitized classification and internal numeric PK enter the queue.
                record_subscription_denial.delay(b.id, denial_reason(params.get("hub.reason")))
                return Response(status_code=204, headers=headers)
    except Exception:
        logger.error("Subscription callback processing failed count=1")
        return Response(status_code=503, headers=headers)
    logger.info("Subscription verification challenge answered count=1")
    return Response(content=challenge, media_type="text/plain", headers=headers)


@app.post("/webhook/youtube")
async def receive_atom_feed(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature: str = Header(None)
):
    """
    実際に「配信始まったよ！」という通知が飛んでくる場所。
    ※このPubSubHubbub経由の通知はライブ「開始」検知が本質的に遅い
    (詳細はlive_poller.py/polling_tasks.py側の即時ポーリングを参照)。
    ここでは新着動画の検知経路として維持しつつ、応答を即座に返すことに専念する。
    """
    body_bytes = await request.body()

    # 1. 署名検証 (セキュリティ)
    # ★以前は署名ヘッダが無い場合に素通りさせていた(fail-open)。ヘッダを付けずに
    #   POSTするだけで偽の配信通知を全サーバーへ流せる状態だったため、secret を
    #   設定しているときは署名の無い要求も必ず拒否する。
    #   ハブは購読時に hub.secret を渡していれば必ず X-Hub-Signature を付けてくる。
    if Config.YOUTUBE_WEBHOOK_SECRET:
        if not x_hub_signature:
            logger.warning("No signature provided; rejected")
            return Response(status_code=403)
        if not verify_signature(body_bytes, x_hub_signature):
            logger.warning("Signature verification failed!")
            return Response(status_code=403)

    # 2. XMLデータの解析
    xml_data = body_bytes.decode('utf-8')

    # 受け取ったXMLを生データのままログファイルに追記保存
    # (ディスクI/Oでレスポンスを遅らせないよう、応答を返した後に実行させる)
    background_tasks.add_task(append_raw_feed_log, xml_data)

    try:
        video_data = parse_video_entry(xml_data)
    except FeedParseError as e:
        logger.error(f"Error parsing XML: {e}")
        background_tasks.add_task(
            append_structured_feed_log,
            {"status": "parse_error", "error": str(e), "raw_xml": xml_data},
        )
        return Response(content="OK", status_code=200)

    if video_data is None:
        # 更新データがない（削除通知などの場合）
        background_tasks.add_task(
            append_structured_feed_log, {"status": "no_entries", "raw_xml": xml_data}
        )
        return Response(content="No entries", status_code=200)

    logger.info(f"Received notification for video: {video_data['title']}")
    background_tasks.add_task(
        append_structured_feed_log, {"status": "parsed", **video_data}
    )

    # 3. 裏方のタスク(Celery)に仕事を投げる
    # ここで処理を終わらせないとYouTubeがタイムアウト判定してしまうため
    process_livestream_task.delay(video_data)

    return Response(content="OK", status_code=200)


@app.get("/")
def read_root():
    return {"status": "ok", "message": "YouTube PubSubHubbub Receiver is Ready!"}


# --- Twitch EventSub ---------------------------------------------------------
# EventSubのイベントで受け取る項目(これ以外はキューに載せない)
_TWITCH_EVENT_FIELDS = ("id", "broadcaster_user_id", "broadcaster_user_login",
                        "broadcaster_user_name", "type", "started_at")
# 種類ごとの、イベントに必ず含まれているべき項目と投入先タスク
_TWITCH_EVENT_TASKS = {
    "stream.online": (("id", "broadcaster_user_id"), process_twitch_stream_online_task),
    "stream.offline": (("broadcaster_user_id",), process_twitch_stream_offline_task),
}


def _twitch_subscription_summary(subscription):
    condition = subscription.get("condition") or {}
    return {
        "id": str(subscription.get("id") or ""),
        "type": str(subscription.get("type") or ""),
        "status": str(subscription.get("status") or ""),
        "condition": {"broadcaster_user_id": str(condition.get("broadcaster_user_id") or "")},
        "transport": {"callback": str((subscription.get("transport") or {}).get("callback") or "")},
    }


@app.post("/webhook/twitch")
async def receive_twitch_eventsub(request: Request, background_tasks: BackgroundTasks):
    """Twitch EventSub(Webhook)の受信口。すべてのメッセージを署名検証してから扱い、
    通知の送信などはCeleryへ渡して即座に2xxを返す(Twitchは数秒で応答が無いと失敗扱いにする)。"""
    headers = {"X-Content-Type-Options": "nosniff"}
    if not Config.twitch_enabled():
        return Response(status_code=404, headers=headers)
    body = await request.body()
    message_id = request.headers.get("Twitch-Eventsub-Message-Id")
    timestamp = request.headers.get("Twitch-Eventsub-Message-Timestamp")
    signature = request.headers.get("Twitch-Eventsub-Message-Signature")
    message_type = request.headers.get("Twitch-Eventsub-Message-Type")

    if not twitch_eventsub.verify_eventsub_signature(message_id, timestamp, body, signature):
        logger.warning("Twitch EventSub signature verification failed count=1")
        return Response(status_code=403, headers=headers)
    if not twitch_eventsub.is_fresh(timestamp):
        # 署名は正しいが10分より古い(リプレイの可能性)。処理しないがTwitchの失敗には数えさせない。
        logger.warning("Twitch EventSub stale message ignored count=1")
        return Response(status_code=204, headers=headers)
    try:
        payload = json.loads(body)
        subscription = payload.get("subscription") or {}
    except (ValueError, AttributeError):
        return Response(status_code=400, headers=headers)

    if message_type == "webhook_callback_verification":
        challenge = payload.get("challenge")
        if not isinstance(challenge, str):
            return Response(status_code=400, headers=headers)
        try:
            allowed = await run_in_threadpool(twitch_eventsub.verification_allowed, subscription)
        except Exception:
            logger.error("Twitch EventSub verification processing failed count=1")
            return Response(status_code=503, headers=headers)
        if not allowed:
            logger.warning("Twitch EventSub verification for unregistered channel rejected count=1")
            return Response(status_code=404, headers=headers)
        background_tasks.add_task(twitch_eventsub.mark_verified, _twitch_subscription_summary(subscription))
        return Response(content=challenge, media_type="text/plain", headers=headers)

    if message_type not in ("notification", "revocation"):
        return Response(status_code=204, headers=headers)

    # 同じMessage-Idの再送は1回しか処理しない。
    try:
        first_delivery = await run_in_threadpool(twitch_eventsub.claim_message, message_id)
    except Exception:
        logger.error("Twitch EventSub message dedup failed count=1")
        return Response(status_code=503, headers=headers)
    if not first_delivery:
        logger.info("Twitch EventSub duplicate message ignored count=1")
        return Response(status_code=204, headers=headers)

    try:
        if message_type == "revocation":
            handle_twitch_revocation_task.delay(_twitch_subscription_summary(subscription))
        elif subscription.get("type") in _TWITCH_EVENT_TASKS:
            required, task = _TWITCH_EVENT_TASKS[subscription["type"]]
            event = payload.get("event") or {}
            if not all(event.get(key) for key in required):
                return Response(status_code=204, headers=headers)
            task.delay({key: str(event[key]) for key in _TWITCH_EVENT_FIELDS if event.get(key) is not None})
    except Exception:
        # キューに載らなかった。記録を取り消して再送(Twitchは失敗時に再送する)で拾い直す。
        logger.error("Twitch EventSub enqueue failed count=1")
        try:
            await run_in_threadpool(twitch_eventsub.release_message, message_id)
        except Exception:
            logger.error("Twitch EventSub dedup release failed count=1")
        return Response(status_code=503, headers=headers)
    return Response(status_code=204, headers=headers)
