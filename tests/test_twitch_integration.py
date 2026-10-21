"""Twitch EventSub統合とYouTube非回帰のオフラインテスト(偽の認証情報・インメモリDB・外部通信なし)。"""
import os
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test")
os.environ.setdefault("YOUTUBE_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("CALLBACK_URL", "https://example.invalid/webhook/youtube")
import asyncio
import hashlib
import hmac
import json
import logging
import sys
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import requests
from sqlalchemy.exc import IntegrityError
from starlette.background import BackgroundTasks
from starlette.requests import Request

# test_subscription_stability.py は tasks をスタブに差し替える。ここでは本物のYouTube通知処理を検証する。
if not hasattr(sys.modules.get("tasks"), "_count_channel_missing"):
    sys.modules.pop("tasks", None)
import tasks
import main
import polling_tasks
import subscription_tasks
import twitch_api
import twitch_eventsub
import twitch_tasks
import channel_registration
import admin_channels_api
import i18n
from admin_schemas import ChannelCreateRequest
from config import Config
from models import (Base, Broadcaster, GuildSetting, LiveStream, SessionLocal, Subscription,
                    TwitchEventSubMessage, TwitchStreamNotification, engine)

twitch_tasks.youtube_tasks = tasks

SECRET = "twitch-webhook-secret-123"
CALLBACK = "https://example.invalid/webhook/twitch"
TWITCH_CONFIG = dict(TWITCH_CLIENT_ID="client-id-x", TWITCH_CLIENT_SECRET="client-secret-x",
                     TWITCH_WEBHOOK_SECRET=SECRET, TWITCH_CALLBACK_URL=CALLBACK)
YT_CHANNEL = "UC" + "y" * 22
TW_ID = "141981764"
TW_LOGIN = "twitchdev"


def now_ts(delta=0):
    t = datetime.now(timezone.utc) + timedelta(seconds=delta)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + "123456789Z"


def sign(message_id, timestamp, body, secret=SECRET):
    return "sha256=" + hmac.new(secret.encode(), (message_id + timestamp).encode() + body, hashlib.sha256).hexdigest()


def subscription_payload(status="enabled", sub_id="sub-1", broadcaster=TW_ID, callback=CALLBACK,
                         event_type="stream.online"):
    return {"id": sub_id, "status": status, "type": event_type, "version": "1", "cost": 1,
            "condition": {"broadcaster_user_id": broadcaster},
            "transport": {"method": "webhook", "callback": callback},
            "created_at": "2026-09-22T00:00:00Z"}


def both_types(status="enabled", prefix="sub"):
    """stream.online と stream.offline の購読ペイロードを1つずつ。"""
    return [subscription_payload(status, prefix + "-online"),
            subscription_payload(status, prefix + "-offline", event_type="stream.offline")]


def offline_event():
    return {"broadcaster_user_id": TW_ID, "broadcaster_user_login": TW_LOGIN,
            "broadcaster_user_name": "TwitchDev"}


def stream_event(stream_id="9001"):
    return {"id": stream_id, "broadcaster_user_id": TW_ID, "broadcaster_user_login": TW_LOGIN,
            "broadcaster_user_name": "TwitchDev", "type": "live", "started_at": "2026-09-22T10:00:00Z"}


async def _inline_threadpool(func, *args, **kwargs):
    # sqliteのインメモリDBはスレッドごとに別DBになるため、テストでは同じスレッドで実行する
    return func(*args, **kwargs)


class DBTestCase(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        self.patches = [
            patch.multiple(Config, **TWITCH_CONFIG),
            # 外部通信は一切させない(requests.post等もSession.request経由なので全部止まる)
            patch("requests.Session.request", side_effect=AssertionError("Unexpected real network request")),
            patch.object(main, "run_in_threadpool", _inline_threadpool),
            patch("starlette.background.run_in_threadpool", _inline_threadpool),
            patch.object(twitch_tasks.time, "sleep"),
            patch.object(tasks.time, "sleep"),
        ]
        for p in self.patches:
            p.start()
        twitch_api._token.update(value=None, expires_at=0.0)

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()
        Base.metadata.drop_all(engine)

    def add_youtube(self, guild=1, channel=100):
        with SessionLocal() as db:
            db.add(Broadcaster(channel_id=YT_CHANNEL, name="YT"))
            db.flush()
            db.add(Subscription(discord_guild_id=guild, discord_channel_id=channel, youtube_channel_id=YT_CHANNEL))
            db.commit()

    def add_twitch(self, guild=1, channel=200, message=None, **fields):
        with SessionLocal() as db:
            b = Broadcaster(platform="twitch", channel_id=TW_ID, name="TwitchDev", twitch_login=TW_LOGIN,
                            twitch_broadcaster_user_id=TW_ID, **fields)
            db.add(b)
            db.flush()
            db.add(Subscription(platform="twitch", discord_guild_id=guild, discord_channel_id=channel,
                                youtube_channel_id=TW_ID, custom_message=message))
            db.commit()
            return b.id

    def webhook(self, payload, message_type="notification", message_id="msg-1", timestamp=None, signature=None):
        body = json.dumps(payload).encode()
        timestamp = timestamp or now_ts()
        signature = signature or sign(message_id, timestamp, body)
        headers = [(b"twitch-eventsub-message-id", message_id.encode()),
                   (b"twitch-eventsub-message-timestamp", timestamp.encode()),
                   (b"twitch-eventsub-message-signature", signature.encode()),
                   (b"twitch-eventsub-message-type", message_type.encode()),
                   (b"content-type", b"application/json")]
        sent = {"done": False}

        async def receive():
            if sent["done"]:
                return {"type": "http.disconnect"}
            sent["done"] = True
            return {"type": "http.request", "body": body, "more_body": False}

        request = Request({"type": "http", "method": "POST", "path": "/webhook/twitch", "headers": headers}, receive)
        background = BackgroundTasks()
        response = asyncio.run(main.receive_twitch_eventsub(request, background))
        asyncio.run(background())
        return response


# --- マイグレーション後のスキーマ・既存YouTubeデータ -------------------------------------

class SchemaTests(DBTestCase):
    def test_existing_style_rows_default_to_youtube(self):
        # 旧コードと同じく platform を指定せずに作った行は youtube になる
        self.add_youtube()
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).one().platform, "youtube")
            sub = db.query(Subscription).one()
            self.assertEqual(sub.platform, "youtube")
            self.assertEqual(sub.broadcaster.channel_id, YT_CHANNEL)

    def test_same_id_allowed_across_platforms_but_not_within(self):
        with SessionLocal() as db:
            db.add(Broadcaster(platform="youtube", channel_id="same"))
            db.add(Broadcaster(platform="twitch", channel_id="same"))
            db.commit()
            db.add(Broadcaster(platform="twitch", channel_id="same"))
            with self.assertRaises(IntegrityError):
                db.commit()

    def test_unknown_platform_rejected(self):
        with SessionLocal() as db:
            db.add(Broadcaster(platform="niconico", channel_id="x"))
            with self.assertRaises(IntegrityError):
                db.commit()


# --- 既存YouTube通知の非回帰 ------------------------------------------------------------

class YouTubeRegressionTests(DBTestCase):
    def test_youtube_notification_unchanged_and_twitch_subs_not_used(self):
        self.add_youtube(channel=100)
        self.add_twitch(channel=200)
        video = {"channel_id": YT_CHANNEL, "video_id": "abcdefghijk", "title": "YT live",
                 "link": "https://www.youtube.com/watch?v=abcdefghijk", "published": "",
                 "thumbnail_url": "", "author_name": "YT"}
        ok = Mock(status_code=200)
        with patch.object(tasks, "get_best_thumbnail", return_value="https://img/yt.jpg"), \
                patch.object(tasks, "classify_broadcast", return_value={"broadcast_state": "live", "is_members_only": False}), \
                patch.object(tasks.verify_members_only_task, "apply_async"), \
                patch("discord_notifier.requests.post", return_value=ok) as post:
            self.assertEqual(tasks.process_livestream_task.run(dict(video)), "Sent 1 notifications")
            self.assertEqual(tasks.process_livestream_task.run(dict(video)), "Duplicate")
        self.assertEqual(post.call_count, 1)
        self.assertIn("/channels/100/", post.call_args.args[0])
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["content"], "🔴 **YT** が配信を開始しました！ @everyone")
        self.assertEqual(payload["embeds"][0]["footer"]["text"], "YouTube Notification Bot")
        self.assertEqual(payload["embeds"][0]["color"], 16711680)

    def test_live_polling_only_youtube(self):
        self.add_youtube()
        self.add_twitch()
        with patch.object(polling_tasks, "is_live_polling_enabled", return_value=True), \
                patch.object(polling_tasks.poll_channel_live_task, "apply_async") as enqueue:
            polling_tasks.poll_all_channels()
        self.assertEqual([c.kwargs["args"] for c in enqueue.call_args_list], [[YT_CHANNEL]])

    def test_websub_renewal_ignores_twitch_rows(self):
        self.add_youtube()
        self.add_twitch()
        lock = MagicMock()
        lock.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = True
        with patch.object(subscription_tasks, "engine", lock), \
                patch.object(subscription_tasks.time, "sleep"), \
                patch.object(subscription_tasks.process_due_subscriptions, "apply_async"), \
                patch.object(subscription_tasks, "_fetch_hub_status",
                             return_value=("verified", subscription_tasks.utcnow() + timedelta(days=9))) as hub, \
                patch.object(subscription_tasks, "_post_subscribe") as post:
            subscription_tasks._run("hourly")
            subscription_tasks._run()
        hub.assert_called_once_with(YT_CHANNEL)
        post.assert_not_called()
        with SessionLocal() as db:
            tw = db.query(Broadcaster).filter_by(platform="twitch").one()
            self.assertIsNone(tw.subscription_next_check_at)
            self.assertEqual(tw.subscription_state, "unknown")

    def test_youtube_webhook_verification_still_works(self):
        from urllib.parse import urlencode
        self.add_youtube()
        query = urlencode({"hub.mode": "subscribe", "hub.topic": subscription_tasks.topic_url(YT_CHANNEL),
                           "hub.challenge": "yt-challenge"})
        response = main.verify_subscription(Request({"type": "http", "query_string": query.encode()}))
        self.assertEqual(response.body, b"yt-challenge")


# --- Webhook: challenge・署名・鮮度 -----------------------------------------------------------

class TwitchWebhookTests(DBTestCase):
    def test_challenge_returned_verbatim_and_marked_enabled(self):
        pk = self.add_twitch(twitch_subscription_status="webhook_callback_verification_pending")
        challenge = "pogchamp-kappa-360noscope-vohiyo"
        response = self.webhook({"challenge": challenge, "subscription": subscription_payload("webhook_callback_verification_pending")},
                                message_type="webhook_callback_verification")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), challenge)
        self.assertTrue(response.headers["content-type"].startswith("text/plain"))
        with SessionLocal() as db:
            b = db.get(Broadcaster, pk)
            self.assertEqual((b.twitch_subscription_status, b.twitch_subscription_id), ("enabled", "sub-1"))

    def test_challenge_for_unregistered_channel_or_foreign_callback_rejected(self):
        self.add_twitch()
        for sub in (subscription_payload(broadcaster="999"), subscription_payload(callback="https://evil.invalid/cb")):
            response = self.webhook({"challenge": "x", "subscription": sub}, message_type="webhook_callback_verification")
            self.assertEqual(response.status_code, 404)

    def test_signature_vector_and_tampering(self):
        body = b'{"a":1}'
        good = sign("id-1", "2026-09-22T00:00:00Z", body)
        self.assertTrue(twitch_eventsub.verify_eventsub_signature("id-1", "2026-09-22T00:00:00Z", body, good))
        self.assertFalse(twitch_eventsub.verify_eventsub_signature("id-2", "2026-09-22T00:00:00Z", body, good))
        self.assertFalse(twitch_eventsub.verify_eventsub_signature("id-1", "2026-09-22T00:00:01Z", body, good))
        self.assertFalse(twitch_eventsub.verify_eventsub_signature("id-1", "2026-09-22T00:00:00Z", b'{"a":2}', good))
        self.assertFalse(twitch_eventsub.verify_eventsub_signature("id-1", "2026-09-22T00:00:00Z", body, None))
        self.assertFalse(twitch_eventsub.verify_eventsub_signature("id-1", "2026-09-22T00:00:00Z", body,
                                                                   sign("id-1", "2026-09-22T00:00:00Z", body, "other-secret-xx")))

    def test_invalid_signature_rejected_without_processing(self):
        self.add_twitch()
        payload = {"subscription": subscription_payload(), "event": stream_event()}
        with patch.object(main.process_twitch_stream_online_task, "delay") as enqueue:
            response = self.webhook(payload, signature="sha256=" + "0" * 64)
            self.assertEqual(response.status_code, 403)
            challenge = self.webhook({"challenge": "x", "subscription": subscription_payload()},
                                     message_type="webhook_callback_verification", signature="sha256=bad")
            self.assertEqual(challenge.status_code, 403)
        enqueue.assert_not_called()
        with SessionLocal() as db:
            self.assertEqual(db.query(TwitchEventSubMessage).count(), 0)

    def test_stale_and_future_messages_not_processed(self):
        self.add_twitch()
        payload = {"subscription": subscription_payload(), "event": stream_event()}
        with patch.object(main.process_twitch_stream_online_task, "delay") as enqueue:
            self.assertEqual(self.webhook(payload, message_id="old", timestamp=now_ts(-601)).status_code, 204)
            self.assertEqual(self.webhook(payload, message_id="future", timestamp=now_ts(601)).status_code, 204)
        enqueue.assert_not_called()

    def test_timestamp_parsing(self):
        parsed = twitch_eventsub.parse_timestamp("2026-09-22T01:02:03.123456789Z")
        self.assertEqual(parsed, datetime(2026, 9, 22, 1, 2, 3, 123456, tzinfo=timezone.utc))
        self.assertEqual(twitch_eventsub.parse_timestamp("2026-09-22T10:02:03+09:00").hour, 1)
        self.assertIsNone(twitch_eventsub.parse_timestamp("yesterday"))

    def test_disabled_when_not_configured(self):
        with patch.object(Config, "TWITCH_WEBHOOK_SECRET", None):
            self.assertEqual(self.webhook({"challenge": "x"}, message_type="webhook_callback_verification").status_code, 404)


# --- stream.online → 通知送信、重複抑止 ------------------------------------------------------

class TwitchNotificationTests(DBTestCase):
    def test_notification_enqueues_sanitized_event(self):
        self.add_twitch()
        event = dict(stream_event(), extra_field="ignored")
        with patch.object(main.process_twitch_stream_online_task, "delay") as enqueue:
            response = self.webhook({"subscription": subscription_payload(), "event": event})
        self.assertEqual(response.status_code, 204)
        enqueue.assert_called_once_with(stream_event())

    def test_duplicate_message_id_processed_once(self):
        self.add_twitch()
        payload = {"subscription": subscription_payload(), "event": stream_event()}
        with patch.object(main.process_twitch_stream_online_task, "delay") as enqueue:
            self.assertEqual(self.webhook(payload, message_id="same").status_code, 204)
            self.assertEqual(self.webhook(payload, message_id="same").status_code, 204)
        enqueue.assert_called_once()

    def test_enqueue_failure_releases_message_for_twitch_retry(self):
        self.add_twitch()
        payload = {"subscription": subscription_payload(), "event": stream_event()}
        with patch.object(main.process_twitch_stream_online_task, "delay", side_effect=[ConnectionError(), None]) as enqueue:
            self.assertEqual(self.webhook(payload, message_id="retry-me").status_code, 503)
            self.assertEqual(self.webhook(payload, message_id="retry-me").status_code, 204)
        self.assertEqual(enqueue.call_count, 2)

    def test_stream_online_sends_notification_like_youtube(self):
        self.add_youtube(channel=100)
        self.add_twitch(channel=200)
        ok = Mock(status_code=204)
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "Dev stream!"}), \
                patch("discord_notifier.requests.post", return_value=ok) as post:
            result = twitch_tasks.process_twitch_stream_online_task.run(stream_event())
        self.assertEqual(result, "Sent 1 notifications")
        self.assertIn("/channels/200/", post.call_args.args[0])
        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["content"], "🔴 **TwitchDev** が配信を開始しました！ @everyone")
        embed = payload["embeds"][0]
        self.assertEqual(embed["title"], "Dev stream!")
        self.assertEqual(embed["url"], "https://www.twitch.tv/twitchdev")
        self.assertEqual(embed["author"]["name"], "TwitchDev")
        self.assertEqual(embed["footer"]["text"], "Twitch Notification Bot")
        self.assertTrue(embed["image"]["url"].startswith(
            "https://static-cdn.jtvnw.net/previews-ttv/live_user_twitchdev-1280x720.jpg"))

    def test_custom_message_placeholders(self):
        self.add_twitch(message="{name} started {title} {link} @here")
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "T"}), \
                patch("discord_notifier.requests.post", return_value=Mock(status_code=200)) as post:
            twitch_tasks.process_twitch_stream_online_task.run(stream_event())
        self.assertEqual(post.call_args.kwargs["json"]["content"], "TwitchDev started T https://www.twitch.tv/twitchdev @here")

    def test_same_stream_notified_once_even_with_new_message_id(self):
        self.add_twitch()
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "T"}), \
                patch("discord_notifier.requests.post", return_value=Mock(status_code=200)) as post:
            self.assertEqual(twitch_tasks.process_twitch_stream_online_task.run(stream_event("77")), "Sent 1 notifications")
            self.assertEqual(twitch_tasks.process_twitch_stream_online_task.run(stream_event("77")), "Duplicate")
            self.assertEqual(twitch_tasks.process_twitch_stream_online_task.run(stream_event("78")), "Sent 1 notifications")
        self.assertEqual(post.call_count, 2)
        with SessionLocal() as db:
            self.assertEqual(sorted(r.video_id for r in db.query(LiveStream)), ["twitch:77", "twitch:78"])

    def test_title_fallback_inactive_and_login_rename(self):
        pk = self.add_twitch()
        with patch.object(twitch_api, "get_channel_info", side_effect=twitch_api.TwitchAPIError("channels_http_503", True)), \
                patch("discord_notifier.requests.post", return_value=Mock(status_code=200)) as post:
            event = dict(stream_event("5"), broadcaster_user_login="newlogin")
            twitch_tasks.process_twitch_stream_online_task.run(event)
            self.assertEqual(post.call_args.kwargs["json"]["embeds"][0]["title"], "配信")
            with SessionLocal() as db:
                b = db.get(Broadcaster, pk)
                self.assertEqual(b.twitch_login, "newlogin")
                b.is_active = False
                db.commit()
            self.assertEqual(twitch_tasks.process_twitch_stream_online_task.run(stream_event("6")),
                             "Inactive channel, skipped")

    def test_missing_discord_channel_counts_and_removes(self):
        self.add_twitch()
        missing = Mock(status_code=404, text="Unknown Channel")
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "T"}), \
                patch("discord_notifier.requests.post", return_value=missing):
            twitch_tasks.process_twitch_stream_online_task.run(stream_event("1"))
            twitch_tasks.process_twitch_stream_online_task.run(stream_event("2"))
        with SessionLocal() as db:
            self.assertEqual(db.query(Subscription).filter_by(platform="twitch").count(), 0)


# --- 登録(DB保存と購読作成)・購読の保守 ------------------------------------------------------

class TwitchRegistrationTests(DBTestCase):
    def mock_api(self, existing=(), create_status="webhook_callback_verification_pending"):
        user = {"id": TW_ID, "login": TW_LOGIN, "display_name": "TwitchDev"}
        subs = list(existing)
        return [patch.object(twitch_api, "get_user_by_login", return_value=user),
                patch.object(twitch_api, "list_subscriptions",
                             side_effect=lambda bid, event_type=None: [
                                 s for s in subs if event_type in (None, s["type"])]),
                patch.object(twitch_api, "create_subscription",
                             side_effect=lambda bid, event_type: subscription_payload(
                                 create_status, "created-" + event_type, event_type=event_type)),
                patch.object(twitch_api, "delete_eventsub_subscription")]

    def run_with(self, mocks, func, *args):
        started = [m.start() for m in mocks]
        try:
            return func(*args), started
        finally:
            for m in mocks:
                m.stop()

    def test_register_saves_rows_and_creates_subscription(self):
        result, (_, _, create, _) = self.run_with(
            self.mock_api(), channel_registration.register_twitch_channel,
            "https://www.twitch.tv/TwitchDev", 10, 20, "hi {name}")
        # 配信開始と配信終了の両方を購読する(終了はアーカイブへの差し替えに使う)
        self.assertEqual([c.args for c in create.call_args_list],
                         [(TW_ID, "stream.online"), (TW_ID, "stream.offline")])
        self.assertTrue(result.created_channel)
        self.assertEqual(result.destination, "created")
        self.assertEqual(result.subscription_status, "webhook_callback_verification_pending")
        with SessionLocal() as db:
            b = db.query(Broadcaster).one()
            self.assertEqual((b.platform, b.channel_id, b.twitch_login, b.twitch_broadcaster_user_id, b.name),
                             ("twitch", TW_ID, TW_LOGIN, TW_ID, "TwitchDev"))
            self.assertEqual(b.twitch_subscription_id, "created-stream.online")
            self.assertEqual(b.twitch_offline_subscription_id, "created-stream.offline")
            self.assertEqual(b.twitch_offline_subscription_status, "webhook_callback_verification_pending")
            sub = db.query(Subscription).one()
            self.assertEqual((sub.platform, sub.discord_guild_id, sub.discord_channel_id, sub.custom_message),
                             ("twitch", 10, 20, "hi {name}"))

    def test_reregister_is_duplicate_and_does_not_create_again(self):
        self.run_with(self.mock_api(), channel_registration.register_twitch_channel, TW_LOGIN, 10, 20, None)
        result, (_, _, create, _) = self.run_with(
            self.mock_api(existing=both_types()), channel_registration.register_twitch_channel,
            TW_LOGIN, 10, 20, None)
        create.assert_not_called()
        self.assertFalse(result.created_channel)
        self.assertEqual(result.destination, "unchanged")
        self.assertEqual(result.subscription_status, "enabled")
        # 同じサーバーから別の通知先 → /add_youtube と同じく更新扱い
        result, _ = self.run_with(self.mock_api(existing=both_types()),
                                  channel_registration.register_twitch_channel, TW_LOGIN, 10, 21, None)
        self.assertEqual(result.destination, "updated")
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).count(), 1)
            self.assertEqual(db.query(Subscription).one().discord_channel_id, 21)

    def test_same_name_youtube_and_twitch_are_separate(self):
        with SessionLocal() as db:
            db.add(Broadcaster(channel_id=TW_ID, name="YouTube row with same id"))
            db.commit()
        result, _ = self.run_with(self.mock_api(), channel_registration.register_twitch_channel, TW_LOGIN, 1, 2, None)
        self.assertTrue(result.created_channel)
        with SessionLocal() as db:
            self.assertEqual(sorted(b.platform for b in db.query(Broadcaster)), ["twitch", "youtube"])

    def test_unknown_channel_and_bad_input_and_api_error(self):
        with patch.object(twitch_api, "get_user_by_login", return_value=None):
            with self.assertRaises(channel_registration.RegistrationError) as raised:
                channel_registration.register_twitch_channel("nobody_here")
            self.assertEqual(raised.exception.http_status, 404)
            self.assertIn("見つかりません", raised.exception.message)
        with self.assertRaises(channel_registration.RegistrationError) as raised:
            channel_registration.register_twitch_channel("https://example.com/foo")
        self.assertEqual(raised.exception.http_status, 422)
        with patch.object(twitch_api, "get_user_by_login", side_effect=twitch_api.TwitchAPIError("users_http_503", True)):
            with self.assertRaises(channel_registration.RegistrationError) as raised:
                channel_registration.register_twitch_channel(TW_LOGIN)
            self.assertEqual(raised.exception.http_status, 502)
        with patch.object(Config, "TWITCH_CLIENT_SECRET", None):
            with self.assertRaises(channel_registration.RegistrationError) as raised:
                channel_registration.register_twitch_channel(TW_LOGIN)
            self.assertEqual(raised.exception.code, "twitch_not_configured")
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).count(), 0)

    def test_subscription_failure_keeps_registration_and_reports_reason(self):
        mocks = self.mock_api()
        mocks[2] = patch.object(twitch_api, "create_subscription",
                                side_effect=twitch_api.TwitchAPIError("eventsub_create_http_400", False, 400))
        result, _ = self.run_with(mocks, channel_registration.register_twitch_channel, TW_LOGIN, 1, 2, None)
        self.assertEqual(result.subscription_status, "error")
        self.assertIn("コールバックURL", channel_registration.describe_error(result.subscription_error))
        with SessionLocal() as db:
            b = db.query(Broadcaster).one()
            self.assertEqual((b.twitch_subscription_status, b.twitch_subscription_error),
                             ("error", "eventsub_create_http_400"))

    def test_conflict_adopts_existing_subscription(self):
        pk = self.add_twitch()
        mocks = self.mock_api()
        raced = {"stream.online": "raced-online", "stream.offline": "raced-offline"}
        mocks[1] = patch.object(twitch_api, "list_subscriptions", side_effect=lambda bid, event_type=None: (
            [] if event_type not in seen and not seen.add(event_type)
            else [subscription_payload("enabled", raced[event_type], event_type=event_type)]))
        mocks[2] = patch.object(twitch_api, "create_subscription",
                                side_effect=twitch_api.TwitchAPIError("eventsub_create_http_409", False, 409))
        seen = set()
        result, _ = self.run_with(mocks, twitch_eventsub.ensure_subscription, pk)
        self.assertEqual(result.status, "enabled")
        with SessionLocal() as db:
            b = db.get(Broadcaster, pk)
            self.assertEqual(b.twitch_subscription_id, "raced-online")
            self.assertEqual(b.twitch_offline_subscription_id, "raced-offline")

    def test_failed_subscription_is_replaced_and_duplicates_removed(self):
        pk = self.add_twitch()
        failed = both_types("notification_failures_exceeded", "old")
        result, (_, _, create, delete) = self.run_with(
            self.mock_api(existing=failed), twitch_eventsub.ensure_subscription, pk)
        self.assertEqual([c.args[0] for c in delete.call_args_list], ["old-online", "old-offline"])
        self.assertEqual(create.call_count, 2)
        dupes = [subscription_payload("webhook_callback_verification_pending", "a"),
                 subscription_payload("enabled", "b"),
                 subscription_payload("enabled", "foreign", callback="https://other.invalid/cb"),
                 subscription_payload("enabled", "c", event_type="stream.offline")]
        result, (_, _, create, delete) = self.run_with(self.mock_api(existing=dupes), twitch_eventsub.ensure_subscription, pk)
        create.assert_not_called()
        delete.assert_called_once_with("a")  # 他サービス宛(foreign)は消さない
        self.assertEqual(result.status, "enabled")

    def test_recreate_replaces_enabled_subscription_but_not_foreign(self):
        pk = self.add_twitch()
        existing = both_types("enabled", "ours") + [
            subscription_payload("enabled", "foreign", callback="https://other.invalid/cb")]
        result, (_, _, create, delete) = self.run_with(
            self.mock_api(existing=existing), twitch_eventsub.ensure_subscription, pk, False, True)
        self.assertEqual([c.args[0] for c in delete.call_args_list], ["ours-online", "ours-offline"])
        self.assertEqual(create.call_count, 2)
        self.assertEqual(result.status, "webhook_callback_verification_pending")

    def test_reconcile_skips_inactive_and_prunes_old_message_ids(self):
        self.add_twitch()
        with SessionLocal() as db:
            db.add(TwitchEventSubMessage(message_id="old", received_at=datetime.now(timezone.utc) - timedelta(days=2)))
            db.add(TwitchEventSubMessage(message_id="new", received_at=datetime.now(timezone.utc)))
            db.add(Broadcaster(platform="twitch", channel_id="555", is_active=False))
            db.commit()
        result, (_, listing, create, _) = self.run_with(self.mock_api(), twitch_tasks.reconcile_twitch_subscriptions.run)
        self.assertEqual([c.args[0] for c in listing.call_args_list], [TW_ID, TW_ID])
        self.assertEqual(result, {"webhook_callback_verification_pending": 1})
        with SessionLocal() as db:
            self.assertEqual([m.message_id for m in db.query(TwitchEventSubMessage)], ["new"])


class TwitchRevocationTests(DBTestCase):
    def test_revocation_logged_recorded_and_resubscribe_scheduled(self):
        pk = self.add_twitch(twitch_subscription_id="sub-1", twitch_subscription_status="enabled")
        revoked = subscription_payload("notification_failures_exceeded", "sub-1")
        with patch.object(main.handle_twitch_revocation_task, "delay",
                          side_effect=lambda s: twitch_tasks.handle_twitch_revocation_task.run(s)), \
                patch.object(twitch_tasks.ensure_twitch_subscription_task, "apply_async") as resubscribe, \
                self.assertLogs("twitch_eventsub", level="ERROR") as logs:
            response = self.webhook({"subscription": revoked}, message_type="revocation", message_id="rev-1")
        self.assertEqual(response.status_code, 204)
        self.assertIn("status=notification_failures_exceeded", logs.output[0])
        resubscribe.assert_called_once_with(args=[pk], countdown=twitch_tasks.REVOCATION_RESUBSCRIBE_DELAY_SECONDS)
        with SessionLocal() as db:
            b = db.get(Broadcaster, pk)
            self.assertEqual((b.twitch_subscription_status, b.twitch_subscription_id, b.twitch_subscription_error),
                             ("notification_failures_exceeded", None, "revoked_notification_failures_exceeded"))

    def test_user_removed_is_not_resubscribed_automatically(self):
        pk = self.add_twitch(twitch_subscription_id="sub-1")
        with patch.object(twitch_tasks.ensure_twitch_subscription_task, "apply_async") as resubscribe:
            twitch_tasks.handle_twitch_revocation_task.run(subscription_payload("user_removed", "sub-1"))
        resubscribe.assert_not_called()
        with patch.object(twitch_api, "list_subscriptions") as listing:
            self.assertEqual(twitch_eventsub.ensure_subscription(pk).status, "user_removed")
        listing.assert_not_called()

    def test_stale_revocation_of_replaced_subscription_ignored(self):
        pk = self.add_twitch(twitch_subscription_id="new-sub", twitch_subscription_status="enabled")
        self.assertIsNone(twitch_eventsub.record_revocation(subscription_payload("notification_failures_exceeded", "old-sub")))
        with SessionLocal() as db:
            self.assertEqual(db.get(Broadcaster, pk).twitch_subscription_status, "enabled")


# --- 配信終了 → アーカイブ(VOD)へのリンク差し替え ---------------------------------------------

VOD = {"id": "2880826925", "stream_id": "9001", "url": "https://www.twitch.tv/videos/2880826925",
       "duration": "2h49m53s", "created_at": "2026-09-22T05:10:09Z",
       "thumbnail_url": "https://cdn.invalid/vod-%{width}x%{height}.jpg"}


class TwitchArchiveTests(DBTestCase):
    def send_notification(self, message_id=12345, channel=200):
        self.add_twitch(channel=channel)
        posted = Mock(status_code=200, json=Mock(return_value={"id": str(message_id)}))
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "Dev stream!"}), \
                patch("discord_notifier.requests.post", return_value=posted):
            twitch_tasks.process_twitch_stream_online_task.run(stream_event())

    def test_sent_message_ids_are_recorded(self):
        self.send_notification()
        with SessionLocal() as db:
            row = db.query(TwitchStreamNotification).one()
            self.assertEqual((row.stream_id, row.broadcaster_user_id, row.discord_channel_id,
                              row.discord_message_id, row.archived),
                             ("9001", TW_ID, 200, 12345, False))

    def test_offline_webhook_enqueues_task(self):
        self.add_twitch()
        with patch.object(main.process_twitch_stream_offline_task, "delay") as enqueue:
            response = self.webhook({"subscription": subscription_payload(event_type="stream.offline"),
                                     "event": offline_event()}, message_id="off-1")
        self.assertEqual(response.status_code, 204)
        enqueue.assert_called_once_with(offline_event())

    def test_archive_replaces_link_in_original_messages(self):
        self.send_notification()
        edited = Mock(status_code=200)
        with patch.object(twitch_api, "get_archive_video", return_value=VOD) as lookup, \
                patch("discord_notifier.requests.patch", return_value=edited) as edit:
            result = twitch_tasks.process_twitch_stream_offline_task.run(offline_event())
        self.assertEqual(result, "Updated 1/1 notifications")
        lookup.assert_called_once_with(TW_ID, "9001")
        self.assertIn("/channels/200/messages/12345", edit.call_args.args[0])
        embed = edit.call_args.kwargs["json"]["embeds"][0]
        self.assertEqual(embed["url"], VOD["url"])
        self.assertEqual(embed["title"], "【アーカイブ】Dev stream!")
        self.assertEqual(embed["image"]["url"], "https://cdn.invalid/vod-1280x720.jpg")
        self.assertEqual(embed["fields"][0]["value"], "2h49m53s")
        self.assertIn("配信終了", embed["footer"]["text"])
        self.assertEqual(embed["author"]["name"], "TwitchDev")
        with SessionLocal() as db:
            self.assertTrue(db.query(TwitchStreamNotification).one().archived)

    def test_only_latest_stream_is_updated(self):
        self.send_notification(message_id=1)
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "second"}), \
                patch("discord_notifier.requests.post",
                      return_value=Mock(status_code=200, json=Mock(return_value={"id": "2"}))):
            twitch_tasks.process_twitch_stream_online_task.run(stream_event("9002"))
        with patch.object(twitch_api, "get_archive_video", return_value=dict(VOD, stream_id="9002")), \
                patch("discord_notifier.requests.patch", return_value=Mock(status_code=200)) as edit:
            twitch_tasks.process_twitch_stream_offline_task.run(offline_event())
        self.assertEqual(len(edit.call_args_list), 1)
        self.assertIn("/messages/2", edit.call_args.args[0])
        with SessionLocal() as db:
            archived = {row.stream_id: row.archived for row in db.query(TwitchStreamNotification)}
        self.assertEqual(archived, {"9001": False, "9002": True})

    def test_archive_not_ready_retries_then_gives_up(self):
        self.send_notification()
        with patch.object(twitch_api, "get_archive_video", return_value=None), \
                patch("discord_notifier.requests.patch") as edit, \
                patch.object(twitch_tasks.process_twitch_stream_offline_task, "apply_async") as retry:
            for attempt, delay in enumerate(twitch_tasks.ARCHIVE_RETRY_DELAYS):
                self.assertEqual(twitch_tasks.process_twitch_stream_offline_task.run(offline_event(), attempt),
                                 "Archive not ready, retrying")
                self.assertEqual(retry.call_args.kwargs, {"args": [offline_event(), attempt + 1], "countdown": delay})
            # 保存設定オフなどで最後まで見つからない場合は、元の通知をそのまま残して打ち切る
            self.assertEqual(twitch_tasks.process_twitch_stream_offline_task.run(
                offline_event(), len(twitch_tasks.ARCHIVE_RETRY_DELAYS)), "Archive unavailable")
            self.assertEqual(retry.call_count, len(twitch_tasks.ARCHIVE_RETRY_DELAYS))
        edit.assert_not_called()
        with SessionLocal() as db:
            self.assertTrue(db.query(TwitchStreamNotification).one().archived)

    def test_api_failure_is_retried_and_deleted_message_is_dropped(self):
        self.send_notification()
        with patch.object(twitch_api, "get_archive_video",
                          side_effect=twitch_api.TwitchAPIError("videos_http_500", True)), \
                patch.object(twitch_tasks.process_twitch_stream_offline_task, "apply_async") as retry:
            self.assertEqual(twitch_tasks.process_twitch_stream_offline_task.run(offline_event()),
                             "Archive not ready, retrying")
        retry.assert_called_once()
        # 通知が削除済み(404)なら、差し替えを諦めて記録を閉じる
        with patch.object(twitch_api, "get_archive_video", return_value=VOD), \
                patch("discord_notifier.requests.patch", return_value=Mock(status_code=404, text="{}")):
            self.assertEqual(twitch_tasks.process_twitch_stream_offline_task.run(offline_event()),
                             "Updated 0/1 notifications")
        with SessionLocal() as db:
            self.assertTrue(db.query(TwitchStreamNotification).one().archived)

    def test_offline_without_notifications_is_noop(self):
        self.add_twitch()
        with patch.object(twitch_api, "get_archive_video") as lookup:
            self.assertEqual(twitch_tasks.process_twitch_stream_offline_task.run(offline_event()),
                             "No notification to update")
        lookup.assert_not_called()

    def test_offline_subscription_revocation_and_verification(self):
        pk = self.add_twitch(twitch_offline_subscription_id="off-1", twitch_offline_subscription_status="enabled")
        with patch.object(twitch_tasks.ensure_twitch_subscription_task, "apply_async") as resubscribe:
            twitch_tasks.handle_twitch_revocation_task.run(
                subscription_payload("notification_failures_exceeded", "off-1", event_type="stream.offline"))
        resubscribe.assert_called_once()
        with SessionLocal() as db:
            b = db.get(Broadcaster, pk)
            self.assertEqual(b.twitch_offline_subscription_status, "notification_failures_exceeded")
            self.assertIsNone(b.twitch_offline_subscription_id)
            self.assertIsNone(b.twitch_subscription_status)  # 配信開始側は無傷
        response = self.webhook({"challenge": "c", "subscription": subscription_payload(
            "webhook_callback_verification_pending", "off-2", event_type="stream.offline")},
            message_type="webhook_callback_verification")
        self.assertEqual(response.body.decode(), "c")
        with SessionLocal() as db:
            b = db.get(Broadcaster, pk)
            self.assertEqual((b.twitch_offline_subscription_status, b.twitch_offline_subscription_id),
                             ("enabled", "off-2"))

    def test_old_notification_records_are_pruned(self):
        self.send_notification()
        with SessionLocal() as db:
            row = db.query(TwitchStreamNotification).one()
            row.created_at = datetime.now(timezone.utc) - timedelta(days=31)
            db.commit()
        with patch.object(twitch_api, "list_subscriptions", return_value=both_types()), \
                patch.object(twitch_api, "create_subscription"), \
                patch.object(twitch_api, "delete_eventsub_subscription"):
            twitch_tasks.reconcile_twitch_subscriptions.run()
        with SessionLocal() as db:
            self.assertEqual(db.query(TwitchStreamNotification).count(), 0)


# --- Twitch APIクライアント ------------------------------------------------------------------

class TwitchApiTests(DBTestCase):
    def response(self, status, body=None):
        return Mock(status_code=status, json=Mock(return_value=body or {}), text="SECRET-BODY")

    def test_token_cached_and_refreshed_on_401(self):
        token1, token2 = self.response(200, {"access_token": "t1", "expires_in": 5000}), \
            self.response(200, {"access_token": "t2", "expires_in": 5000})
        users = self.response(200, {"data": [{"id": TW_ID, "login": TW_LOGIN}]})
        with patch("requests.request", side_effect=[token1, users, users, self.response(401), token2, users]) as req:
            self.assertEqual(twitch_api.get_user_by_login("TwitchDev")["id"], TW_ID)
            twitch_api.get_user_by_login(TW_LOGIN)  # 2回目はトークン再取得なし
            twitch_api.get_user_by_login(TW_LOGIN)  # 401 → 取り直して再試行
        urls = [c.args[1] for c in req.call_args_list]
        self.assertEqual(urls.count(twitch_api.TOKEN_URL), 2)
        self.assertEqual(req.call_args_list[1].kwargs["params"], {"login": "twitchdev"})
        self.assertEqual(req.call_args.kwargs["headers"]["Authorization"], "Bearer t2")
        self.assertFalse(req.call_args.kwargs["allow_redirects"])

    def test_create_payload_and_errors_do_not_leak_secrets(self):
        token = self.response(200, {"access_token": "tok", "expires_in": 5000})
        created = self.response(202, {"data": [subscription_payload("webhook_callback_verification_pending")]})
        with patch("requests.request", side_effect=[token, created]) as req:
            twitch_api.create_subscription(TW_ID, "stream.online")
        body = req.call_args.kwargs["json"]
        self.assertEqual(body["type"], "stream.online")
        self.assertEqual(body["condition"], {"broadcaster_user_id": TW_ID})
        self.assertEqual(body["transport"], {"method": "webhook", "callback": CALLBACK, "secret": SECRET})
        with patch("requests.request", side_effect=requests.Timeout("https://id.twitch.tv/?client_secret=client-secret-x")), \
                self.assertLogs(level="DEBUG") as logs:
            logging.getLogger("probe").info("probe")
            with self.assertRaises(twitch_api.TwitchAPIError) as raised:
                twitch_api.get_user_by_login(TW_LOGIN)
        self.assertEqual(str(raised.exception), "network_timeout")
        for secret in ("client-secret-x", SECRET, "tok"):
            self.assertFalse(any(secret in line for line in logs.output))
        with patch("requests.request", side_effect=[token, self.response(429)]):
            with self.assertRaises(twitch_api.TwitchAPIError) as raised:
                twitch_api._token.update(value=None, expires_at=0.0)
                twitch_api.list_subscriptions(TW_ID)
            self.assertTrue(raised.exception.transient)
            self.assertNotIn("SECRET", str(raised.exception))

    def test_list_filters_type_and_paginates(self):
        token = self.response(200, {"access_token": "tok", "expires_in": 5000})
        page1 = self.response(200, {"data": [subscription_payload(sub_id="a"),
                                             dict(subscription_payload(sub_id="x"), type="channel.follow")],
                                    "pagination": {"cursor": "c1"}})
        page2 = self.response(200, {"data": [subscription_payload(sub_id="b")], "pagination": {}})
        with patch("requests.request", side_effect=[token, page1, page2]) as req:
            subs = twitch_api.list_subscriptions(TW_ID, "stream.online")
        self.assertEqual([s["id"] for s in subs], ["a", "b"])
        self.assertEqual(req.call_args.kwargs["params"], {"user_id": TW_ID, "after": "c1"})

    def test_login_parsing_and_config_validation(self):
        cases = {"TwitchDev": "twitchdev", "@twitchdev": "twitchdev", "https://www.twitch.tv/TwitchDev": "twitchdev",
                 "twitch.tv/twitchdev/videos": None, "https://m.twitch.tv/abc_12?x=1": "abc_12",
                 "https://www.twitch.tv/directory": None, "https://youtube.com/abc": None, "bad name": None, "": None}
        for value, expected in cases.items():
            self.assertEqual(twitch_eventsub.parse_twitch_login(value), expected, value)
        self.assertIsNone(Config.twitch_config_error())
        for key, value, code in (("TWITCH_CALLBACK_URL", "http://example.invalid/cb", "twitch_callback_url_invalid"),
                                 ("TWITCH_CALLBACK_URL", "https://example.invalid:8195/cb", "twitch_callback_url_invalid"),
                                 ("TWITCH_WEBHOOK_SECRET", "short", "twitch_webhook_secret_invalid")):
            with patch.object(Config, key, value):
                self.assertEqual(Config.twitch_config_error(), code)
        with patch.object(Config, "TWITCH_CALLBACK_URL", "https://example.invalid:443/cb"):
            self.assertIsNone(Config.twitch_config_error())


# --- 管理API --------------------------------------------------------------------------------

class AdminApiTests(DBTestCase):
    def test_create_twitch_list_and_duplicate(self):
        user = {"id": TW_ID, "login": TW_LOGIN, "display_name": "TwitchDev"}
        body = ChannelCreateRequest(platform="twitch", identifier="twitch.tv/twitchdev",
                                    discord_guild_id="123456789012345678", discord_channel_id="223456789012345678")
        with patch.object(twitch_api, "get_user_by_login", return_value=user), \
                patch.object(twitch_api, "list_subscriptions", return_value=[]), \
                patch.object(twitch_api, "create_subscription",
                             return_value=subscription_payload("webhook_callback_verification_pending")):
            created = admin_channels_api.create_channel(body)
            with self.assertRaises(admin_channels_api.HTTPException) as raised:
                admin_channels_api.create_channel(body)
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(created["channel"]["platform"], "twitch")
        self.assertEqual(created["channel"]["url"], "https://www.twitch.tv/twitchdev")
        with SessionLocal() as db:
            self.assertEqual(db.query(Subscription).one().discord_channel_id, 223456789012345678)
        self.add_youtube()
        listed = admin_channels_api.get_channels()
        self.assertEqual([c["platform"] for c in listed], ["youtube", "twitch"])

    def test_create_youtube_enqueues_websub(self):
        with patch.object(channel_registration.request_initial_subscription, "delay") as enqueue:
            created = admin_channels_api.create_channel(
                ChannelCreateRequest(platform="youtube", identifier="https://www.youtube.com/channel/" + YT_CHANNEL))
        enqueue.assert_called_once()
        self.assertEqual((created["channel"]["platform"], created["channel"]["channel_id"]), ("youtube", YT_CHANNEL))

    def test_validation_switches_by_platform(self):
        from pydantic import ValidationError
        for platform, identifier in (("youtube", "twitchdev"), ("twitch", YT_CHANNEL + "/x/y"),
                                     ("twitch", "https://www.youtube.com/channel/" + YT_CHANNEL)):
            with self.assertRaises(ValidationError):
                ChannelCreateRequest(platform=platform, identifier=identifier)
        with self.assertRaises(ValidationError):
            ChannelCreateRequest(platform="twitch", identifier="ok_name", discord_guild_id="123456789012345678")
        self.assertEqual(ChannelCreateRequest(platform="twitch", identifier=" ok_name ").identifier, "ok_name")

    def test_patch_defaults_to_youtube_for_old_clients(self):
        self.add_youtube()
        from admin_schemas import ChannelUpdateRequest
        result = admin_channels_api.patch_channel(YT_CHANNEL, ChannelUpdateRequest(is_active=False), platform="youtube")
        self.assertEqual((result["channel_id"], result["is_active"]), (YT_CHANNEL, False))
        with self.assertRaises(admin_channels_api.HTTPException):
            admin_channels_api.patch_channel(YT_CHANNEL, ChannelUpdateRequest(is_active=False), platform="twitch")

    def test_resubscribe_reports_reason(self):
        self.add_twitch()
        with patch.object(twitch_api, "list_subscriptions",
                          side_effect=twitch_api.TwitchAPIError("token_http_403", False, 403)):
            with self.assertRaises(admin_channels_api.HTTPException) as raised:
                admin_channels_api.resubscribe_channel(TW_ID, platform="twitch")
        self.assertEqual(raised.exception.status_code, 502)
        self.assertIn("Client ID / Client Secret", raised.exception.detail)


# --- Botコマンド ----------------------------------------------------------------------------

class BotCommandTests(DBTestCase):
    def setUp(self):
        super().setUp()
        import bot
        self.bot = bot
        to_thread = patch.object(bot.asyncio, "to_thread", _inline_threadpool)
        to_thread.start()
        self.addCleanup(to_thread.stop)

    def interaction(self, can_post=True):
        perms = SimpleNamespace(view_channel=can_post, send_messages=can_post, embed_links=can_post)
        channel = SimpleNamespace(id=555, mention="#live", permissions_for=Mock(return_value=perms))
        return SimpleNamespace(response=SimpleNamespace(defer=AsyncMock(), is_done=Mock(return_value=False),
                                                        send_message=AsyncMock()),
                               followup=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(delete=AsyncMock()))),
                               channel=channel, channel_id=555,
                               guild_id=42, guild=SimpleNamespace(me=object(), name="Guild")), channel

    def test_command_name_and_description(self):
        command = self.bot.bot.tree.get_command("add_twitch")
        self.assertIn("Twitchを追加", command.description)
        self.assertEqual([p.name for p in command.parameters], ["url", "channel", "message"])
        self.assertIsNotNone(self.bot.bot.tree.get_command("add_youtube"))
        # 代理設定できるよう、実行者の権限は問わない(/add_youtube と同じ)
        self.assertEqual(command.checks, [])
        self.assertIsNone(command.default_permissions)

    def test_add_twitch_registers_and_reports(self):
        interaction, _ = self.interaction()
        user = {"id": TW_ID, "login": TW_LOGIN, "display_name": "TwitchDev"}
        with patch.object(twitch_api, "get_user_by_login", return_value=user), \
                patch.object(twitch_api, "list_subscriptions", return_value=[]), \
                patch.object(twitch_api, "create_subscription",
                             return_value=subscription_payload("webhook_callback_verification_pending")):
            asyncio.run(self.bot.add_twitch.callback(interaction, "https://twitch.tv/twitchdev"))
        text = interaction.followup.send.call_args.args[0]
        self.assertIn("新規登録", text)
        self.assertNotIn("検証待ち", text)
        self.assertNotIn("EventSub購読", text)
        self.assertFalse(text.endswith("\n"))
        self.assertTrue(interaction.followup.send.call_args.kwargs["wait"])
        interaction.followup.send.return_value.delete.assert_awaited_once_with(delay=10)
        with SessionLocal() as db:
            sub = db.query(Subscription).one()
            self.assertEqual((sub.platform, sub.discord_guild_id, sub.discord_channel_id), ("twitch", 42, 555))

    def test_new_registration_with_warning_is_kept(self):
        interaction, _ = self.interaction()
        user = {"id": TW_ID, "login": TW_LOGIN, "display_name": "TwitchDev"}
        with patch.object(twitch_api, "get_user_by_login", return_value=user), \
                patch.object(twitch_api, "list_subscriptions",
                             side_effect=twitch_api.TwitchAPIError("eventsub_list_http_503", True)):
            asyncio.run(self.bot.add_twitch.callback(interaction, TW_LOGIN))
        self.assertIn("⚠️", interaction.followup.send.call_args.args[0])
        interaction.followup.send.return_value.delete.assert_not_awaited()

    def test_add_youtube_new_registration_deleted_but_update_kept(self):
        interaction, _ = self.interaction()
        interaction.guild = SimpleNamespace(me=object(), name="Guild")
        url = "https://www.youtube.com/channel/" + YT_CHANNEL
        with patch.object(self.bot.request_initial_subscription, "delay"):
            asyncio.run(self.bot.add_youtube.callback(interaction, url))
            self.assertIn("新規登録しました", interaction.followup.send.call_args.args[0])
            self.assertTrue(interaction.followup.send.call_args.kwargs["wait"])
            interaction.followup.send.return_value.delete.assert_awaited_once_with(delay=10)
            interaction, _ = self.interaction()
            asyncio.run(self.bot.add_youtube.callback(interaction, url, None, "new message"))
        self.assertIn("設定を更新しました", interaction.followup.send.call_args.args[0])
        self.assertNotIn("wait", interaction.followup.send.call_args.kwargs)
        interaction.followup.send.return_value.delete.assert_not_awaited()

    def test_add_twitch_error_messages(self):
        interaction, _ = self.interaction(can_post=False)
        asyncio.run(self.bot.add_twitch.callback(interaction, TW_LOGIN))
        self.assertIn("投稿権限がありません", interaction.followup.send.call_args.args[0])
        interaction, _ = self.interaction()
        with patch.object(twitch_api, "get_user_by_login", return_value=None):
            asyncio.run(self.bot.add_twitch.callback(interaction, "ghost_user"))
        interaction.followup.send.return_value.delete.assert_not_awaited()  # エラーは消さない
        self.assertIn("見つかりません", interaction.followup.send.call_args.args[0])
        interaction, _ = self.interaction()
        with patch.object(twitch_api, "get_user_by_login", side_effect=twitch_api.TwitchAPIError("users_http_500", True)):
            asyncio.run(self.bot.add_twitch.callback(interaction, TW_LOGIN))
        self.assertIn("一時的なエラー", interaction.followup.send.call_args.args[0])
        interaction, _ = self.interaction()
        asyncio.run(self.bot.add_twitch_error(interaction, self.bot.app_commands.NoPrivateMessage()))
        self.assertIn("サーバー内で実行", interaction.response.send_message.call_args.args[0])


# --- サーバーごとの言語設定(/language) ------------------------------------------------------

class GuildLanguageTests(DBTestCase):
    def add_second_twitch_guild(self, guild=2, channel=300):
        with SessionLocal() as db:
            db.add(Subscription(platform="twitch", discord_guild_id=guild, discord_channel_id=channel,
                                youtube_channel_id=TW_ID))
            db.commit()

    def send_twitch(self):
        posted = Mock(status_code=200, json=Mock(return_value={"id": "1"}))
        with patch.object(twitch_api, "get_channel_info", return_value={"title": "Dev stream!"}), \
                patch("discord_notifier.requests.post", return_value=posted) as post:
            twitch_tasks.process_twitch_stream_online_task.run(stream_event())
        return {call.args[0]: call.kwargs["json"]["content"] for call in post.call_args_list}

    def test_twitch_notification_uses_each_guild_language(self):
        self.add_twitch(guild=1, channel=200)
        self.add_second_twitch_guild(guild=2, channel=300)
        i18n.set_guild_language(1, "en")
        contents = self.send_twitch()
        self.assertEqual(contents[main_url(200)], "🔴 **TwitchDev** has started a live stream! @everyone")
        # 未設定のサーバーは従来どおりの日本語
        self.assertEqual(contents[main_url(300)], "🔴 **TwitchDev** が配信を開始しました！ @everyone")

    def test_archive_embed_uses_each_guild_language(self):
        self.add_twitch(guild=1, channel=200)
        self.add_second_twitch_guild(guild=2, channel=300)
        i18n.set_guild_language(2, "ko")
        self.send_twitch()
        with patch.object(twitch_api, "get_archive_video", return_value=VOD), \
                patch("discord_notifier.requests.patch", return_value=Mock(status_code=200)) as edit:
            twitch_tasks.process_twitch_stream_offline_task.run(offline_event())
        titles = {call.args[0].split("/channels/")[1].split("/")[0]: call.kwargs["json"]["embeds"][0]["title"]
                  for call in edit.call_args_list}
        self.assertEqual(titles, {"200": "【アーカイブ】Dev stream!", "300": "[다시보기] Dev stream!"})

    def test_members_only_followup_uses_guild_language(self):
        self.add_youtube(guild=1, channel=100)
        i18n.set_guild_language(1, "zh")
        with patch.object(tasks, "fetch_video_details", return_value={"statistics": {}}), \
                patch("discord_notifier.requests.post", return_value=Mock(status_code=200)) as post:
            tasks.verify_members_only_task.run("vid", YT_CHANNEL, "t", "https://y.invalid/v", "YT", False)
        self.assertTrue(post.call_args.kwargs["json"]["content"].startswith("🔒 补充：刚才 **YT** 的直播"))

    def test_missing_settings_table_falls_back_to_japanese(self):
        # bot の init_db() より前(guild_settings がまだ無い)でも通知は日本語で送れる
        self.add_twitch(guild=1, channel=200)
        GuildSetting.__table__.drop(engine)
        contents = self.send_twitch()
        self.assertEqual(contents[main_url(200)], "🔴 **TwitchDev** が配信を開始しました！ @everyone")


def main_url(channel_id):
    return f"https://discord.com/api/v10/channels/{channel_id}/messages"


if __name__ == "__main__":
    unittest.main(verbosity=2)
