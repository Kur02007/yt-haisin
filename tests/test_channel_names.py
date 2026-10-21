"""チャンネル名がDBに入る経路と、/remove_* の一覧表示のテスト(外部通信なし)。"""
import os
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test")
os.environ.setdefault("YOUTUBE_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("CALLBACK_URL", "https://example.invalid/webhook/youtube")
import sys
import unittest
from unittest.mock import Mock, patch

if not hasattr(sys.modules.get("tasks"), "_update_broadcaster_name"):
    sys.modules.pop("tasks", None)
import tasks
import channel_registration
import youtube_api
from channel_registration import (UNKNOWN_NAME, list_guild_registrations,
                                  register_youtube_channel, unregister_channel)
from models import (Base, PLATFORM_TWITCH, PLATFORM_YOUTUBE, Broadcaster, SessionLocal,
                    Subscription, engine)

GUILD = 111122223333444455
DISCORD_CHANNEL = 222233334444555566
CHANNEL_ID = "UC0topOTAhXlxevzudmI8w7g"
CHANNEL_NAME = "紫月まな"
HANDLE_URL = "https://www.youtube.com/@shizuki_mana"


def api_response(name=CHANNEL_NAME):
    return Mock(status_code=200, text="", **{"json.return_value": {
        "items": [{"id": CHANNEL_ID, "snippet": {"title": name}}]}})


class NameOnRegistrationTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        youtube_api.clear_handle_cache()
        self.addCleanup(youtube_api.clear_handle_cache)
        self.enqueue = patch.object(channel_registration, "request_initial_subscription")
        self.enqueue.start()
        self.addCleanup(self.enqueue.stop)

    def register(self, identifier):
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                patch("youtube_api.requests.get", return_value=api_response()) as get:
            result = register_youtube_channel(identifier, GUILD, DISCORD_CHANNEL)
        return result, get

    def test_handle_registration_stores_name(self):
        """ハンドルURLでの登録は、変換と同じ1クオータで得た名前をDBに残す。"""
        result, get = self.register(HANDLE_URL)
        self.assertEqual(get.call_count, 1)
        with SessionLocal() as db:
            b = db.query(Broadcaster).filter(Broadcaster.channel_id == CHANNEL_ID).one()
            self.assertEqual(b.name, CHANNEL_NAME)
        self.assertEqual(result.channel_id, CHANNEL_ID)

    def test_channel_id_registration_keeps_unknown(self):
        """チャンネルID指定では名前が分からないので、従来どおりUnknownで登録する。"""
        with patch("youtube_api.requests.get", side_effect=AssertionError("クオータを使ってはいけない")):
            register_youtube_channel("https://www.youtube.com/channel/" + CHANNEL_ID,
                                     GUILD, DISCORD_CHANNEL)
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).one().name, UNKNOWN_NAME)

    def test_handle_registration_fills_existing_unknown(self):
        """先にID登録されてUnknownだった行も、ハンドル登録時に名前が入る。"""
        with SessionLocal() as db:
            db.add(Broadcaster(platform=PLATFORM_YOUTUBE, channel_id=CHANNEL_ID, name=UNKNOWN_NAME))
            db.commit()
        self.register(HANDLE_URL)
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).one().name, CHANNEL_NAME)

    def test_existing_real_name_is_not_overwritten_by_registration(self):
        with SessionLocal() as db:
            db.add(Broadcaster(platform=PLATFORM_YOUTUBE, channel_id=CHANNEL_ID, name="手動でつけた名前"))
            db.commit()
        self.register(HANDLE_URL)
        with SessionLocal() as db:
            self.assertEqual(db.query(Broadcaster).one().name, "手動でつけた名前")


class NameFromNotificationTest(unittest.TestCase):
    """通知(WebSubのフィード/ライブポーリング)で判明した名前をDBへ反映する。"""

    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)

    def update(self, stored_name, author_name):
        with SessionLocal() as db:
            db.add(Broadcaster(platform=PLATFORM_YOUTUBE, channel_id=CHANNEL_ID, name=stored_name))
            db.commit()
        with SessionLocal() as db:
            b = db.query(Broadcaster).one()
            tasks._update_broadcaster_name(db, b, author_name)
        with SessionLocal() as db:
            return db.query(Broadcaster).one().name

    def test_unknown_is_filled_from_feed(self):
        self.assertEqual(self.update(UNKNOWN_NAME, CHANNEL_NAME), CHANNEL_NAME)

    def test_rename_is_followed(self):
        self.assertEqual(self.update("古い名前", CHANNEL_NAME), CHANNEL_NAME)

    def test_unknown_author_does_not_overwrite(self):
        for author in (UNKNOWN_NAME, "", None):
            with self.subTest(author=author):
                Base.metadata.drop_all(engine)
                Base.metadata.create_all(engine)
                self.assertEqual(self.update(CHANNEL_NAME, author), CHANNEL_NAME)

    def test_missing_broadcaster_is_ignored(self):
        with SessionLocal() as db:
            tasks._update_broadcaster_name(db, None, CHANNEL_NAME)  # 例外を出さない

    def test_db_failure_does_not_break_notification(self):
        with SessionLocal() as db:
            db.add(Broadcaster(platform=PLATFORM_YOUTUBE, channel_id=CHANNEL_ID, name=UNKNOWN_NAME))
            db.commit()
        with SessionLocal() as db:
            b = db.query(Broadcaster).one()
            with patch.object(db, "commit", side_effect=tasks.SQLAlchemyError("boom")), \
                    patch.object(db, "rollback") as rollback:
                tasks._update_broadcaster_name(db, b, CHANNEL_NAME)  # 通知処理を止めない
            rollback.assert_called_once()


class RemovalListingTest(unittest.TestCase):
    """/remove_youtube・/remove_twitch の候補表示。"""

    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)

    def add(self, platform, channel_id, name, login=None):
        with SessionLocal() as db:
            db.add(Broadcaster(platform=platform, channel_id=channel_id, name=name,
                               twitch_login=login))
            db.flush()
            db.add(Subscription(platform=platform, discord_guild_id=GUILD,
                                discord_channel_id=DISCORD_CHANNEL, youtube_channel_id=channel_id))
            db.commit()

    def labels(self, platform=PLATFORM_YOUTUBE):
        with SessionLocal() as db:
            return [label for _, label in list_guild_registrations(db, GUILD, platform)]

    def test_shows_channel_name_not_id(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, CHANNEL_NAME)
        self.assertEqual(self.labels(), [CHANNEL_NAME])

    def test_falls_back_to_id_while_name_unknown(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, UNKNOWN_NAME)
        self.assertEqual(self.labels(), [CHANNEL_ID])

    def test_twitch_falls_back_to_login(self):
        self.add(PLATFORM_TWITCH, "555", None, login="streamer")
        self.assertEqual(self.labels(PLATFORM_TWITCH), ["streamer"])

    def test_duplicate_names_get_id_for_distinction(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, CHANNEL_NAME)
        other = "UC" + "z" * 22
        self.add(PLATFORM_YOUTUBE, other, CHANNEL_NAME)
        self.assertEqual(self.labels(),
                         ["%s (%s)" % (CHANNEL_NAME, CHANNEL_ID), "%s (%s)" % (CHANNEL_NAME, other)])

    def test_remove_twitch_by_autocomplete_value_and_names(self):
        """候補の値(数値ID)・表示名・ログイン名・URLのどれでも削除できる。"""
        # Twitchは表示名とログイン名が違うことがある(ボドカさん / vodkavdk)
        for identifier in ("48962167", "ボドカさん", "vodkavdk", "VodkaVDK",
                           "https://www.twitch.tv/vodkavdk", "@vodkavdk"):
            with self.subTest(identifier=identifier):
                Base.metadata.drop_all(engine)
                Base.metadata.create_all(engine)
                self.add(PLATFORM_TWITCH, "48962167", "ボドカさん", login="vodkavdk")
                result = unregister_channel(PLATFORM_TWITCH, identifier, GUILD)
                self.assertEqual(result.channel_id, "48962167")

    def test_remove_youtube_by_channel_name(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, CHANNEL_NAME)
        self.assertEqual(unregister_channel(PLATFORM_YOUTUBE, CHANNEL_NAME, GUILD).channel_id,
                         CHANNEL_ID)

    def test_ambiguous_name_asks_to_pick_from_list(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, CHANNEL_NAME)
        self.add(PLATFORM_YOUTUBE, "UC" + "z" * 22, CHANNEL_NAME)
        with self.assertRaises(channel_registration.RegistrationError) as ctx:
            unregister_channel(PLATFORM_YOUTUBE, CHANNEL_NAME, GUILD)
        self.assertEqual(ctx.exception.code, "ambiguous_name")

    def test_name_of_another_guild_is_not_matched(self):
        self.add(PLATFORM_TWITCH, "48962167", "ボドカさん", login="vodkavdk")
        with self.assertRaises(channel_registration.RegistrationError) as ctx:
            unregister_channel(PLATFORM_TWITCH, "ボドカさん", GUILD + 1)
        self.assertEqual(ctx.exception.code, "invalid_input")

    def test_unknown_twitch_id_is_not_treated_as_channel(self):
        """登録されていない数値を渡しても、別チャンネルを消したりしない。"""
        self.add(PLATFORM_TWITCH, "48962167", "ボドカさん", login="vodkavdk")
        with self.assertRaises(channel_registration.RegistrationError):
            unregister_channel(PLATFORM_TWITCH, "99999999", GUILD)

    def test_removal_message_uses_name(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, CHANNEL_NAME)
        result = unregister_channel(PLATFORM_YOUTUBE, CHANNEL_ID, GUILD)
        self.assertEqual(result.name, CHANNEL_NAME)

    def test_labels_fit_discord_limit(self):
        self.add(PLATFORM_YOUTUBE, CHANNEL_ID, "あ" * 150)
        self.assertTrue(all(len(label) <= 150 for label in self.labels()))


if __name__ == "__main__":
    unittest.main()
