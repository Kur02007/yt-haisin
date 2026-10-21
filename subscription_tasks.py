"""Hub-confirmed leases; subscription HTTP requests originate only in these tasks.

Confirmed state and pending renewal are independent: HTTP 202 neither invalidates
an existing lease nor extends it. Durable due times survive worker/Redis restarts.
Only fixed error codes and aggregate counts may be logged (never URLs/exceptions).
"""
import logging
import random
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html import unescape

import requests
from sqlalchemy import text

from celery_app import celery_app
from config import Config
from models import PLATFORM_YOUTUBE, Broadcaster, SessionLocal, engine

logger = logging.getLogger(__name__)
# WebSub購読はYouTubeのみ。Twitch行は twitch_tasks.py が EventSub で管理する。
YOUTUBE_ACTIVE = (Broadcaster.platform == PLATFORM_YOUTUBE, Broadcaster.is_active.is_not(False))
HUB_SUBSCRIBE_URL = "https://pubsubhubbub.appspot.com/subscribe"
HUB_DETAILS_URL = "https://pubsubhubbub.appspot.com/subscription-details"
LEASE_SECONDS = 864000
LOCK_KEY = 8726030920  # Project-specific PostgreSQL session advisory lock.
VERIFY_DELAYS = (60, 180, 300)  # GET only; no re-POST during confirmation.
METRICS = ("attempted", "accepted", "extended", "maintained", "transient_errors",
           "permanent_errors", "warning", "critical", "expired", "pending", "failed")


def utcnow():
    return datetime.now(timezone.utc)


def utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def topic_url(channel_id):
    return "https://www.youtube.com/xml/feeds/videos.xml?channel_id=" + channel_id


class HubError(Exception):
    def __init__(self, code, transient=False):
        self.code = code
        self.transient = transient
        super().__init__(code)


def _request(method, url, **kwargs):
    # Never redirect secret query/form data to another host. Suppress HTTP debug
    # output, which otherwise includes the full query string.
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    try:
        return requests.request(method, url, timeout=Config.SUBSCRIPTION_REQUEST_TIMEOUT_SECONDS,
                                allow_redirects=False, **kwargs)
    except requests.Timeout:
        raise HubError("network_timeout", True) from None
    except requests.ConnectionError:
        raise HubError("network_connection", True) from None
    except requests.RequestException:
        raise HubError("network_request", True) from None


def _http_error(response, operation):
    code = response.status_code
    # Even 429 is a 4xx: do not automatically retry a rejected request.
    return HubError("%s_http_%d" % (operation, code), 500 <= code <= 599)


def _parse_status(body):
    fragments = [unescape(s).strip() for s in re.sub(r"<[^>]+>", "\n", body).splitlines()]
    fragments = [s for s in fragments if s]
    state, expires = None, None
    for i, fragment in enumerate(fragments[:-1]):
        label = fragment.rstrip(":").strip().lower()
        if label == "state":
            state = fragments[i + 1].lower()
        elif label.startswith("expiration"):
            try:
                expires = utc(parsedate_to_datetime(fragments[i + 1]))
            except (TypeError, ValueError, OverflowError):
                expires = None
    if state not in {"verified", "unverified", "pending", "expired", "denied", "unknown"}:
        raise HubError("status_invalid_response", True)
    return ("pending" if state == "unverified" else state), expires


def _fetch_hub_status(channel_id):
    response = _request("GET", HUB_DETAILS_URL, params={
        "hub.callback": Config.CALLBACK_URL, "hub.topic": topic_url(channel_id),
        "hub.secret": Config.YOUTUBE_WEBHOOK_SECRET,
    })
    try:
        if response.status_code != 200:
            raise _http_error(response, "status")
        return _parse_status(response.text)
    finally:
        response.close()


def _post_subscribe(channel_id):
    response = _request("POST", HUB_SUBSCRIBE_URL, data={
        "hub.mode": "subscribe", "hub.topic": topic_url(channel_id),
        "hub.callback": Config.CALLBACK_URL, "hub.lease_seconds": LEASE_SECONDS,
        "hub.secret": Config.YOUTUBE_WEBHOOK_SECRET,
    })
    try:
        if response.status_code not in (202, 204):
            raise _http_error(response, "request")
    finally:
        response.close()


def _failure(b, error, metrics):
    b.subscription_last_error = error.code
    b.subscription_failure_count = (b.subscription_failure_count or 0) + 1
    metrics["transient_errors" if error.transient else "permanent_errors"] += 1
    if not error.transient:
        # A later successful GET must not silently unblock a rejected POST.
        b.subscription_blocked = True
        b.subscription_renewal_state = "blocked"
    logger.warning("Subscription operation failed code=%s count=1", error.code)


def _defer(b, seconds, action):
    b.subscription_next_check_at = utcnow() + timedelta(seconds=seconds)
    b.subscription_next_action = action


def _retry(b):
    used = b.subscription_retry_count or 0
    if used >= Config.SUBSCRIPTION_RETRY_ATTEMPTS:
        b.subscription_renewal_state = "deferred"
        return
    # Three retries AFTER the initial attempt: 30, 90, 300 seconds by default.
    base = Config.SUBSCRIPTION_RETRY_BASE_SECONDS * (1, 3, 10)[used]
    b.subscription_retry_count = used + 1
    _defer(b, random.uniform(0.8, 1.2) * base, "retry")
    b.subscription_renewal_state = "retry"


def _verify_later(b):
    used = b.subscription_verify_count or 0
    if used < len(VERIFY_DELAYS):
        _defer(b, VERIFY_DELAYS[used], "verify")
        b.subscription_verify_count = used + 1
    else:
        # Retain the baseline so a late confirmation can still be recognized.
        b.subscription_renewal_state = "pending"


def _observe(b, state, expires, now, metrics):
    b.subscription_checked_at = now
    b.subscription_state = state
    b.subscription_expires_at = expires
    if expires is not None and expires <= now:
        b.subscription_state = "expired"
    confirmed = state == "verified" and expires is not None and expires > now
    baseline = utc(b.subscription_renewal_baseline_at)
    if confirmed and b.subscription_pending_since is not None and baseline is not None and expires > baseline:
        # This is the ONLY renewal-success path. Never infer a lease from a POST.
        b.subscription_renewed_at = now
        b.subscription_pending_since = None
        b.subscription_renewal_baseline_at = None
        b.subscription_renewal_state = "confirmed"
        b.subscription_failure_count = 0
        b.subscription_last_error = None
        b.subscription_blocked = False
        metrics["extended"] += 1
        logger.info("Subscription recovery: verified expiry extension count=1")
        return True
    if confirmed:
        metrics["maintained"] += 1
    return False


def _process_one(b, metrics):
    action = b.subscription_next_action or "check"
    b.subscription_next_check_at = None
    b.subscription_next_action = None
    previous_query_failure = (b.subscription_last_error or "").startswith("status_")
    status_ok = False
    try:
        state, expires = _fetch_hub_status(b.channel_id)
        status_ok = True
        if _observe(b, state, expires, utcnow(), metrics):
            return
    except HubError as error:
        if error.code.startswith("network_"):
            error = HubError("status_" + error.code, error.transient)
        _failure(b, error, metrics)
        if not error.transient:
            return

    now = utcnow()
    expires = utc(b.subscription_expires_at)
    if expires is not None and expires <= now and b.subscription_state != "denied":
        b.subscription_state = "expired"
    if b.subscription_blocked:
        return
    if action == "verify":
        # Unchanged verified data is NOT renewal success.
        _verify_later(b)
        return
    if (status_ok and not previous_query_failure and b.subscription_state == "verified"
            and expires is not None and expires > now + timedelta(days=Config.SUBSCRIPTION_RENEW_BEFORE_DAYS)):
        if b.subscription_pending_since is None:
            b.subscription_renewal_state = "idle"
        return

    metrics["attempted"] += 1
    b.subscription_last_attempt_at = now
    try:
        _post_subscribe(b.channel_id)
    except HubError as error:
        _failure(b, error, metrics)
        if error.transient:
            _retry(b)
        return
    metrics["accepted"] += 1
    # Existing DateTime is naive UTC; all new date columns use timestamptz.
    b.last_subscribed_at = utcnow().replace(tzinfo=None)
    if b.subscription_pending_since is None:
        b.subscription_renewal_baseline_at = expires or now
    b.subscription_pending_since = utcnow()
    b.subscription_renewal_state = "pending"
    b.subscription_verify_count = 0
    _verify_later(b)


def _urgent(b, now):
    expires = utc(b.subscription_expires_at)
    return (b.subscription_state in ("expired", "denied") or
            (expires is not None and expires <= now + timedelta(hours=Config.SUBSCRIPTION_CRITICAL_HOURS)))


def _counts(db, metrics):
    now = utcnow()
    for b in db.query(Broadcaster).filter(*YOUTUBE_ACTIVE).all():
        expires = utc(b.subscription_expires_at)
        if expires is not None and expires <= now and b.subscription_state != "denied":
            b.subscription_state = "expired"
        if expires is not None and expires <= now + timedelta(hours=Config.SUBSCRIPTION_WARNING_HOURS):
            metrics["warning"] += 1
        if _urgent(b, now):
            metrics["critical"] += 1
        if b.subscription_state in ("expired", "denied"):
            metrics["expired"] += 1
        if b.subscription_pending_since is not None:
            metrics["pending"] += 1
        if (b.subscription_failure_count or 0) >= 2:
            metrics["failed"] += 1
    db.commit()
    level = logging.ERROR if metrics["critical"] else (
        logging.WARNING if metrics["warning"] or metrics["failed"] or metrics["transient_errors"] or metrics["permanent_errors"] else logging.INFO)
    logger.log(level, "Subscription summary " + " ".join("%s=%d" % (key, metrics[key]) for key in METRICS))


def _run(mode="due", broadcaster_id=None):
    """Serialize subscription jobs across workers without locking polling.

    Each invocation processes at most one channel. A PostgreSQL advisory lock
    prevents overlap; a one-second lock-held gap separates consecutive POSTs.
    ETA jobs are backed by the periodic sweep of durable due dates.
    """
    metrics = Counter()
    with engine.connect() as lock_connection:
        acquired = lock_connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": LOCK_KEY}).scalar()
        if not acquired:
            task = {"hourly": renew_subscriptions, "urgent": urgent_subscriptions,
                    "initial": request_initial_subscription}.get(mode)
            if task is not None:
                task.apply_async(args=[broadcaster_id] if mode == "initial" else [], countdown=random.uniform(3, 8))
            return {"busy": 1}
        db = SessionLocal()
        try:
            now = utcnow()
            if mode != "due":
                query = db.query(Broadcaster).filter(*YOUTUBE_ACTIVE)
                if broadcaster_id is not None:
                    query = query.filter(Broadcaster.id == broadcaster_id)
                for b in query.all():
                    if mode == "urgent" and not _urgent(b, now):
                        continue
                    if b.subscription_next_check_at is not None:
                        continue  # Do not overwrite pending retry/verification jobs.
                    interval = Config.SUBSCRIPTION_URGENT_CHECK_MINUTES if _urgent(b, now) else 55
                    cycle_started = utc(b.subscription_cycle_started_at)
                    if cycle_started and now < cycle_started + timedelta(minutes=interval):
                        continue
                    b.subscription_retry_count = 0
                    b.subscription_verify_count = 0
                    b.subscription_next_check_at = now
                    b.subscription_next_action = "check"
                    b.subscription_cycle_started_at = now
                db.commit()
            b = (db.query(Broadcaster).filter(*YOUTUBE_ACTIVE,
                 Broadcaster.subscription_next_check_at <= utcnow())
                 .order_by(Broadcaster.subscription_next_check_at, Broadcaster.id).first())
            if b is None and mode == "due":
                return {}  # Idle sweep: no duplicate ETA messages or noisy summaries.
            if b is not None:
                try:
                    _process_one(b, metrics)
                    db.commit()
                except Exception:
                    db.rollback()
                    # Exception text can contain SQL parameters or secret URLs.
                    logger.error("Subscription processing internal failure count=1")
                    raise RuntimeError("Subscription processing internal failure") from None
                finally:
                    time.sleep(1.05)
            _counts(db, metrics)
            next_due = (db.query(Broadcaster.subscription_next_check_at)
                        .filter(*YOUTUBE_ACTIVE, Broadcaster.subscription_next_check_at.isnot(None))
                        .order_by(Broadcaster.subscription_next_check_at).first())
            if next_due:
                delay = max(1, (utc(next_due[0]) - utcnow()).total_seconds())
                process_due_subscriptions.apply_async(countdown=delay)
            return dict(metrics)
        finally:
            db.close()
            lock_connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": LOCK_KEY})


@celery_app.task(ignore_result=True)
def renew_subscriptions():
    return _run("hourly")


@celery_app.task(ignore_result=True)
def urgent_subscriptions():
    return _run("urgent")


@celery_app.task(ignore_result=True)
def process_due_subscriptions():
    return _run()


@celery_app.task(ignore_result=True)
def request_initial_subscription(broadcaster_id):
    # Internal numeric PK only: channel IDs never appear in task args/logs.
    return _run("initial", broadcaster_id)


def denial_reason(reason):
    # Untrusted hub.reason can include arbitrary URLs/secrets. Classify only.
    value = (reason or "").lower()
    if "verif" in value:
        return "denied_verification"
    if "topic" in value:
        return "denied_topic"
    if "callback" in value:
        return "denied_callback"
    return "denied_unspecified"


@celery_app.task(ignore_result=True)
def record_subscription_denial(broadcaster_id, reason_code):
    allowed = {"denied_verification", "denied_topic", "denied_callback", "denied_unspecified"}
    reason_code = reason_code if reason_code in allowed else "denied_unspecified"
    with engine.connect() as connection:
        acquired = connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": LOCK_KEY}).scalar()
        if not acquired:
            record_subscription_denial.apply_async(args=[broadcaster_id, reason_code], countdown=5)
            return
        try:
            _record_denial(broadcaster_id, reason_code)
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": LOCK_KEY})


def _record_denial(broadcaster_id, reason_code):
    with SessionLocal() as db:
        b = db.query(Broadcaster).filter(Broadcaster.id == broadcaster_id).with_for_update().first()
        if b is None:
            return
        b.subscription_state = "denied"
        b.subscription_renewal_state = "blocked"
        b.subscription_blocked = True
        b.subscription_last_error = reason_code
        b.subscription_failure_count = (b.subscription_failure_count or 0) + 1
        b.subscription_next_check_at = None
        b.subscription_next_action = None
        db.commit()
        logger.error("Subscription denied: count=1 reason=%s", b.subscription_last_error)
