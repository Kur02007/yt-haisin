"""Twitch EventSub: 署名検証・購読の存在保証・失効処理。

・購読の作成/確認は ensure_subscription() に一本化する(Bot/管理画面/起動時/毎時確認の共通入口)。
・同じ配信者への同時実行は broadcasters 行の SELECT ... FOR UPDATE で直列化し、
  さらにTwitch側の409(同一条件の購読が既にある)も「既存を採用」として扱うため重複作成しない。
・ログには固定コード・status名・内部の数値PKだけを出す(シークレット/トークン/URLは出さない)。
"""
import hashlib
import hmac
import logging
import re
from collections import namedtuple
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import IntegrityError

from config import Config
from models import PLATFORM_TWITCH, Broadcaster, SessionLocal, TwitchEventSubMessage
import twitch_api
from twitch_api import TwitchAPIError

logger = logging.getLogger(__name__)

# Twitch推奨: これより古い(または未来の)メッセージは署名が正しくても処理しない。
MESSAGE_MAX_AGE_SECONDS = 600
# Twitchが通知を送ってくる/送る予定の購読status。これ以外は失効扱い。
ACTIVE_STATUSES = ("enabled", "webhook_callback_verification_pending")
# 配信者アカウント自体が消えた。再購読しても無意味なので明示操作(force)まで止める。
TERMINAL_STATUSES = ("user_removed",)
STATUS_ERROR = "error"

# イベント種類ごとの保存先カラム(id, status, error)。online側は既存の列をそのまま使う。
_COLUMNS = {
    twitch_api.STREAM_ONLINE: ("twitch_subscription_id", "twitch_subscription_status",
                               "twitch_subscription_error"),
    twitch_api.STREAM_OFFLINE: ("twitch_offline_subscription_id", "twitch_offline_subscription_status",
                                "twitch_offline_subscription_error"),
}

EnsureResult = namedtuple("EnsureResult", "status error transient")

_TIMESTAMP_RE = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)$")
_URL_RE = re.compile(r"^(?:https?://)?(?:www\.|m\.)?twitch\.tv/([A-Za-z0-9_]+)/?(?:[?#].*)?$", re.IGNORECASE)
_LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{1,25}$")
# twitch.tv/<ここ> がチャンネルでないURL
_NON_CHANNEL_PATHS = {"directory", "videos", "settings", "search", "downloads", "subscriptions",
                      "inventory", "wallet", "drops", "friends", "messages", "p", "jobs", "turbo"}


def utcnow():
    return datetime.now(timezone.utc)


# --- 入力の解釈 -------------------------------------------------------------

def parse_twitch_login(value):
    """"name" / "@name" / "https://www.twitch.tv/name" からログイン名(小文字)を取り出す。不正ならNone。"""
    text = (value or "").strip()
    match = _URL_RE.match(text)
    if match:
        login = match.group(1)
        if login.lower() in _NON_CHANNEL_PATHS:
            return None
    elif "/" in text or "." in text:
        return None
    else:
        login = text.lstrip("@")
    return login.lower() if _LOGIN_RE.match(login) else None


# --- Webhook署名 ------------------------------------------------------------

def verify_eventsub_signature(message_id, timestamp, body, signature):
    """HMAC-SHA256(secret, message_id + timestamp + 生body) を "sha256=<hex>" と定数時間比較する。"""
    secret = Config.TWITCH_WEBHOOK_SECRET
    if not (secret and message_id and timestamp and signature):
        return False
    expected = "sha256=" + hmac.new(secret.encode(), message_id.encode() + timestamp.encode() + body,
                                    hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature.encode(), expected.encode())


def parse_timestamp(value):
    """Twitchのタイムスタンプ(RFC3339、ナノ秒あり)をUTCのdatetimeへ。解釈できなければNone。"""
    match = _TIMESTAMP_RE.match(value or "")
    if not match:
        return None
    base, fraction, zone = match.groups()
    text = base + "." + (fraction or "0")[:6].ljust(6, "0") + ("+00:00" if zone == "Z" else zone)
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


def is_fresh(timestamp, now=None):
    sent_at = parse_timestamp(timestamp)
    if sent_at is None:
        return False
    return abs(((now or utcnow()) - sent_at).total_seconds()) <= MESSAGE_MAX_AGE_SECONDS


# --- 購読の存在保証 -----------------------------------------------------------

def _record(b, status, subscription_id=None, error=None, event_type=twitch_api.STREAM_ONLINE):
    id_column, status_column, error_column = _COLUMNS[event_type]
    setattr(b, id_column, subscription_id)
    setattr(b, status_column, status)
    setattr(b, error_column, error)
    b.twitch_subscription_checked_at = utcnow()


def _status_of(b, event_type):
    return getattr(b, _COLUMNS[event_type][1])


def _is_ours(sub):
    return (sub.get("transport") or {}).get("callback") == Config.TWITCH_CALLBACK_URL


def _adopt_existing(b, subs, event_type):
    """自分のコールバック宛の有効な購読があれば1つだけ残して採用する。採用したらTrue。"""
    active = [s for s in subs if _is_ours(s) and s.get("status") in ACTIVE_STATUSES]
    if not active:
        return False
    active.sort(key=lambda s: s.get("status") != "enabled")
    keep = active[0]
    for extra in active[1:]:
        twitch_api.delete_eventsub_subscription(extra["id"])
        logger.warning("Twitch EventSub duplicate subscription deleted type=%s broadcaster=%s count=1",
                       event_type, b.id)
    _record(b, keep["status"], keep["id"], event_type=event_type)
    return True


def _ensure_type(b, event_type, force, recreate):
    if _status_of(b, event_type) in TERMINAL_STATUSES and not force:
        return
    subs = twitch_api.list_subscriptions(b.channel_id, event_type)
    if recreate:
        # シークレット変更後などに、有効に見える古い購読も含めて自分宛てを作り直す
        for sub in subs:
            if _is_ours(sub):
                twitch_api.delete_eventsub_subscription(sub["id"])
        subs = [sub for sub in subs if not _is_ours(sub)]
    elif _adopt_existing(b, subs, event_type):
        return
    # 失効済み(notification_failures_exceeded等)の購読は消してから作り直す。
    for sub in subs:
        if _is_ours(sub):
            status = sub.get("status")
            logger.warning("Twitch EventSub inactive subscription found status=%s type=%s broadcaster=%s;"
                           " resubscribing", status, event_type, b.id)
            if status in TERMINAL_STATUSES and not force:
                _record(b, status, None, "revoked_" + status, event_type)
                return
            twitch_api.delete_eventsub_subscription(sub["id"])
    try:
        created = twitch_api.create_subscription(b.channel_id, event_type)
    except TwitchAPIError as error:
        if error.status != 409:
            raise
        # 並行作成などで既にある。改めて一覧を取り、自分宛なら採用する。
        if _adopt_existing(b, twitch_api.list_subscriptions(b.channel_id, event_type), event_type):
            return
        raise TwitchAPIError("eventsub_conflict_other_callback") from None
    _record(b, created.get("status") or "webhook_callback_verification_pending", created.get("id"),
            event_type=event_type)
    logger.info("Twitch EventSub subscription created type=%s status=%s broadcaster=%s",
                event_type, _status_of(b, event_type), b.id)


def _ensure(b, force, recreate):
    """配信開始(通知用)と配信終了(アーカイブ差し替え用)の両方を用意する。"""
    # 配信者アカウントが消えている場合は、どちらの購読も作り直さない。
    if not force and any(_status_of(b, t) in TERMINAL_STATUSES for t in twitch_api.EVENT_TYPES):
        return
    for event_type in twitch_api.EVENT_TYPES:
        _ensure_type(b, event_type, force, recreate)


def ensure_subscription(broadcaster_pk, force=False, recreate=False):
    """指定Twitchチャンネルの stream.online 購読が存在する状態にする。結果はDBにも記録する。
    force=True は利用者の明示操作(再登録・管理画面の再購読)で、user_removed でも再作成を試みる。
    recreate=True は自分宛ての購読を削除して作り直す(管理画面の再購読。シークレット変更時に使う)。"""
    with SessionLocal() as db:
        b = (db.query(Broadcaster)
             .filter(Broadcaster.id == broadcaster_pk, Broadcaster.platform == PLATFORM_TWITCH)
             .with_for_update().first())
        if b is None:
            return EnsureResult(None, "broadcaster_not_found", False)
        try:
            _ensure(b, force or recreate, recreate)
            # 配信開始の購読が通知の本体。終了側の失敗はアーカイブ差し替えだけに影響する。
            result = EnsureResult(b.twitch_subscription_status,
                                  b.twitch_subscription_error or b.twitch_offline_subscription_error, False)
        except TwitchAPIError as error:
            _record(b, STATUS_ERROR, b.twitch_subscription_id, error.code)
            level = logging.WARNING if error.transient else logging.ERROR
            logger.log(level, "Twitch EventSub ensure failed code=%s broadcaster=%s", error.code, b.id)
            result = EnsureResult(STATUS_ERROR, error.code, error.transient)
        db.commit()
        return result


def record_revocation(subscription):
    """Twitchからのrevocation通知を記録する。戻り値: 再購読すべき配信者PK(不要ならNone)。"""
    status = str(subscription.get("status") or "unknown")[:64]
    event_type = subscription.get("type")
    if event_type not in _COLUMNS:
        return None
    broadcaster_user_id = str((subscription.get("condition") or {}).get("broadcaster_user_id") or "")
    with SessionLocal() as db:
        b = (db.query(Broadcaster)
             .filter(Broadcaster.platform == PLATFORM_TWITCH, Broadcaster.channel_id == broadcaster_user_id)
             .with_for_update().first())
        if b is None:
            logger.warning("Twitch EventSub revoked for unknown broadcaster status=%s count=1", status)
            return None
        current_id = getattr(b, _COLUMNS[event_type][0])
        if current_id and current_id != subscription.get("id"):
            # 既に作り直した後に古い購読の失効通知が届いた。現在の購読は健全なので触らない。
            return None
        _record(b, status, None, "revoked_" + status, event_type)
        db.commit()
        # 失効はERROR(監視とDiscordの管理者DMに載せる)。配信者削除は再購読しないのでWARNING。
        level = logging.WARNING if status in TERMINAL_STATUSES else logging.ERROR
        logger.log(level, "Twitch EventSub subscription revoked type=%s status=%s broadcaster=%s",
                   event_type, status, b.id)
        return None if status in TERMINAL_STATUSES or not b.is_active else b.id


def _verification_broadcaster(db, subscription):
    broadcaster_user_id = str((subscription.get("condition") or {}).get("broadcaster_user_id") or "")
    if subscription.get("type") not in _COLUMNS or not _is_ours(subscription):
        return None
    return (db.query(Broadcaster)
            .filter(Broadcaster.platform == PLATFORM_TWITCH, Broadcaster.channel_id == broadcaster_user_id)
            .first())


def verification_allowed(subscription):
    """登録済みチャンネル宛の stream.online 購読の検証要求か(読み取りのみ。行ロックを待たない)。
    登録されていないチャンネルの検証には応答せず、Twitch側で購読を失敗させる。"""
    with SessionLocal() as db:
        return _verification_broadcaster(db, subscription) is not None


def mark_verified(subscription):
    """challengeに応答した購読を enabled として記録する(Twitchは2xx+challenge一致で有効化する)。
    購読作成中の行ロックを待つことがあるため、応答を返した後にバックグラウンドで呼ぶ。"""
    with SessionLocal() as db:
        b = _verification_broadcaster(db, subscription)
        if b is None:
            return False
        _record(b, "enabled", subscription.get("id"), event_type=subscription.get("type"))
        db.commit()
        logger.info("Twitch EventSub verification answered type=%s broadcaster=%s count=1",
                    subscription.get("type"), b.id)
        return True


def prune_messages(db, now=None):
    cutoff = (now or utcnow()) - timedelta(days=1)
    return db.query(TwitchEventSubMessage).filter(TwitchEventSubMessage.received_at < cutoff).delete()


def claim_message(message_id):
    """EventSubメッセージIDを記録する。初見ならTrue、再送(処理済み)ならFalse。"""
    with SessionLocal() as db:
        db.add(TwitchEventSubMessage(message_id=message_id[:128], received_at=utcnow()))
        try:
            db.commit()
            return True
        except IntegrityError:
            db.rollback()
            return False


def release_message(message_id):
    """キュー投入に失敗したときに記録を取り消し、Twitchの再送で処理し直せるようにする。"""
    with SessionLocal() as db:
        db.query(TwitchEventSubMessage).filter(TwitchEventSubMessage.message_id == message_id[:128]).delete()
        db.commit()
