"""1サーバーあたりの登録上限(YouTube・Twitch合計)のオフラインテスト。"""
import os
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test")
os.environ.setdefault("YOUTUBE_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("CALLBACK_URL", "https://example.invalid/webhook/youtube")
import unittest
from unittest.mock import Mock, patch

import channel_registration
import youtube_api
from channel_registration import (RegistrationError, check_guild_capacity,
                                  check_guild_capacity_before_lookup, count_guild_channels,
                                  list_guild_registrations, register_youtube_channel,
                                  register_twitch_channel, unregister_channel)
from config import Config
from models import (Base, PLATFORM_TWITCH, PLATFORM_YOUTUBE, Broadcaster, SessionLocal,
                    Subscription, engine)

GUILD = 111122223333444455
OTHER_GUILD = 999988887777666655
DISCORD_CHANNEL = 222233334444555566


def youtube_id(n):
    return "UC" + str(n).zfill(22)


def add_registration(db, platform, channel_id, guild_id=GUILD):
    """既存の登録(配信者行＋通知先)を直接作る。同じ組み合わせは二重に作らない。"""
    if not db.query(Broadcaster).filter(Broadcaster.platform == platform,
                                        Broadcaster.channel_id == channel_id).first():
        db.add(Broadcaster(platform=platform, channel_id=channel_id, name="existing"))
        db.flush()
    exists = db.query(Subscription).filter(
        Subscription.platform == platform, Subscription.youtube_channel_id == channel_id,
        Subscription.discord_guild_id == guild_id).first()
    if exists is None:
        db.add(Subscription(platform=platform, discord_guild_id=guild_id,
                            discord_channel_id=DISCORD_CHANNEL, youtube_channel_id=channel_id))


def fill_guild(count, guild_id=GUILD, start=0):
    with SessionLocal() as db:
        for i in range(start, start + count):
            add_registration(db, PLATFORM_YOUTUBE, youtube_id(i), guild_id)
        db.commit()


class GuildLimitTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        self.limit = patch.object(Config, "MAX_CHANNELS_PER_GUILD", 20)
        self.limit.start()
        self.addCleanup(self.limit.stop)

    def test_default_limit_is_20(self):
        self.limit.stop()
        try:
            self.assertEqual(Config.MAX_CHANNELS_PER_GUILD, 20)
        finally:
            self.limit.start()

    def test_under_limit_is_allowed(self):
        fill_guild(19)
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 19)
            check_guild_capacity(db, GUILD, PLATFORM_YOUTUBE, youtube_id(99))  # 20個目は通る

    def test_at_limit_rejects_new_channel(self):
        fill_guild(20)
        with SessionLocal() as db:
            with self.assertRaises(RegistrationError) as ctx:
                check_guild_capacity(db, GUILD, PLATFORM_YOUTUBE, youtube_id(99))
        self.assertEqual(ctx.exception.code, "guild_limit_reached")
        self.assertEqual(ctx.exception.http_status, 409)
        self.assertIn("20個まで", ctx.exception.message)

    def test_limit_counts_youtube_and_twitch_together(self):
        """YouTube 15 + Twitch 5 = 20 で上限。どちらのプラットフォームも追加できない。"""
        with SessionLocal() as db:
            for i in range(15):
                add_registration(db, PLATFORM_YOUTUBE, youtube_id(i))
            for i in range(5):
                add_registration(db, PLATFORM_TWITCH, "tw%d" % i)
            db.commit()
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 20)
            for platform, channel_id in ((PLATFORM_YOUTUBE, youtube_id(99)), (PLATFORM_TWITCH, "tw99")):
                with self.subTest(platform=platform), self.assertRaises(RegistrationError):
                    check_guild_capacity(db, GUILD, platform, channel_id)

    def test_existing_registration_can_still_be_updated(self):
        """上限に達していても、既に登録済みチャンネルの通知先変更は通す。"""
        fill_guild(20)
        with SessionLocal() as db:
            check_guild_capacity(db, GUILD, PLATFORM_YOUTUBE, youtube_id(0))

    def test_limit_is_per_guild(self):
        fill_guild(20)
        with SessionLocal() as db:
            check_guild_capacity(db, OTHER_GUILD, PLATFORM_YOUTUBE, youtube_id(99))

    def test_no_guild_is_not_limited(self):
        """通知先を指定しない登録(管理画面からのチャンネル追加のみ)は上限の対象外。"""
        fill_guild(20)
        with SessionLocal() as db:
            check_guild_capacity(db, None, PLATFORM_YOUTUBE, youtube_id(99))


class RegistrationPathTest(unittest.TestCase):
    """実際の登録処理(YouTube/Twitch)が上限で止まり、途中の行を残さないこと。"""

    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        self.limit = patch.object(Config, "MAX_CHANNELS_PER_GUILD", 20)
        self.limit.start()
        self.addCleanup(self.limit.stop)
        fill_guild(20)

    def test_register_youtube_channel_rejected_at_limit(self):
        new_id = youtube_id(99)
        with patch.object(channel_registration, "request_initial_subscription") as enqueue:
            with self.assertRaises(RegistrationError) as ctx:
                register_youtube_channel("https://www.youtube.com/channel/" + new_id,
                                         GUILD, DISCORD_CHANNEL, None)
        self.assertEqual(ctx.exception.code, "guild_limit_reached")
        enqueue.delay.assert_not_called()  # WebSub購読を始めない
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 20)
            # 上限で弾いたチャンネルの配信者行を残さない
            self.assertIsNone(db.query(Broadcaster).filter(
                Broadcaster.platform == PLATFORM_YOUTUBE, Broadcaster.channel_id == new_id).first())

    def test_register_twitch_channel_rejected_at_limit(self):
        user = {"id": "141981764", "login": "twitchdev", "display_name": "TwitchDev"}
        with patch.object(channel_registration, "resolve_twitch_user", return_value=user), \
                patch.object(channel_registration.twitch_eventsub, "ensure_subscription") as ensure:
            with self.assertRaises(RegistrationError) as ctx:
                register_twitch_channel("twitchdev", GUILD, DISCORD_CHANNEL, None)
        self.assertEqual(ctx.exception.code, "guild_limit_reached")
        ensure.assert_not_called()  # EventSub購読を作らない
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 20)
            self.assertIsNone(db.query(Broadcaster).filter(
                Broadcaster.platform == PLATFORM_TWITCH, Broadcaster.channel_id == user["id"]).first())

    def test_existing_channel_update_still_works_at_limit(self):
        """上限でも、既存チャンネルの通知先変更は成功する。"""
        moved_to = DISCORD_CHANNEL + 1
        with patch.object(channel_registration, "request_initial_subscription"):
            result = register_youtube_channel("https://www.youtube.com/channel/" + youtube_id(0),
                                              GUILD, moved_to, "新しいメッセージ")
        self.assertEqual(result.destination, "updated")
        with SessionLocal() as db:
            sub = db.query(Subscription).filter(
                Subscription.discord_guild_id == GUILD,
                Subscription.youtube_channel_id == youtube_id(0)).one()
            self.assertEqual(sub.discord_channel_id, moved_to)
            self.assertEqual(count_guild_channels(db, GUILD), 20)

class QuotaFreeRejectionTest(unittest.TestCase):
    """上限超過のときは、YouTube/TwitchのAPIを呼ぶ前に断る(クオータを使わない)。"""

    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        self.limit = patch.object(Config, "MAX_CHANNELS_PER_GUILD", 20)
        self.limit.start()
        self.addCleanup(self.limit.stop)
        youtube_api.clear_handle_cache()
        self.addCleanup(youtube_api.clear_handle_cache)
        fill_guild(20)

    def test_youtube_handle_rejected_without_api_call(self):
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                patch("youtube_api.requests.get", side_effect=AssertionError("クオータを使ってはいけない")):
            with self.assertRaises(RegistrationError) as ctx:
                register_youtube_channel("https://www.youtube.com/@newchannel", GUILD, DISCORD_CHANNEL)
        self.assertEqual(ctx.exception.code, "guild_limit_reached")
        self.assertIn("チャンネルID", ctx.exception.message)  # ID指定の案内を添える

    def test_twitch_rejected_without_api_call(self):
        with patch.object(channel_registration, "resolve_twitch_user",
                          side_effect=AssertionError("Twitch APIを呼んではいけない")):
            with self.assertRaises(RegistrationError) as ctx:
                register_twitch_channel("https://www.twitch.tv/newstreamer", GUILD, DISCORD_CHANNEL)
        self.assertEqual(ctx.exception.code, "guild_limit_reached")

    def test_registered_twitch_channel_can_still_be_updated_without_api_call(self):
        """上限でも、登録済みTwitchチャンネルはログイン名からDBで特定でき、APIなしで通過判定できる。"""
        with SessionLocal() as db:
            b = db.query(Broadcaster).filter(Broadcaster.platform == PLATFORM_YOUTUBE).first()
            db.add(Broadcaster(platform=PLATFORM_TWITCH, channel_id="555", name="Streamer",
                               twitch_login="streamer"))
            db.flush()
            db.add(Subscription(platform=PLATFORM_TWITCH, discord_guild_id=GUILD,
                                discord_channel_id=DISCORD_CHANNEL, youtube_channel_id="555"))
            db.delete(db.query(Subscription).filter(
                Subscription.youtube_channel_id == b.channel_id).one())  # 20個のまま保つ
            db.commit()
        check_guild_capacity_before_lookup(PLATFORM_TWITCH, "https://www.twitch.tv/streamer", GUILD)

    def test_cached_handle_of_registered_channel_passes(self):
        """ハンドルURLでも、変換キャッシュにあって登録済みなら更新として通る。"""
        registered = youtube_id(0)
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                patch("youtube_api.requests.get", return_value=Mock(
                    status_code=200, text="",
                    **{"json.return_value": {"items": [{"id": registered}]}})):
            youtube_api.resolve_channel_id("https://www.youtube.com/@known")  # キャッシュに載せる
        with patch("youtube_api.requests.get", side_effect=AssertionError("クオータを使ってはいけない")):
            check_guild_capacity_before_lookup(
                PLATFORM_YOUTUBE, "https://www.youtube.com/@known", GUILD)

    def test_under_limit_still_calls_api(self):
        """上限未満なら従来どおりハンドル変換を行う(事前チェックが邪魔をしない)。"""
        with SessionLocal() as db:
            db.delete(db.query(Subscription).filter(
                Subscription.youtube_channel_id == youtube_id(0)).one())
            db.commit()
        new_id = youtube_id(99)
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                patch("youtube_api.requests.get", return_value=Mock(
                    status_code=200, text="", **{"json.return_value": {"items": [{"id": new_id}]}})) as get, \
                patch.object(channel_registration, "request_initial_subscription"):
            result = register_youtube_channel("https://www.youtube.com/@newchannel",
                                              GUILD, DISCORD_CHANNEL)
        self.assertEqual(result.channel_id, new_id)
        self.assertEqual(get.call_count, 1)


class RemoveCommandTest(unittest.TestCase):
    """/remove_youtube・/remove_twitch が使う削除処理。"""

    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        self.limit = patch.object(Config, "MAX_CHANNELS_PER_GUILD", 20)
        self.limit.start()
        self.addCleanup(self.limit.stop)
        youtube_api.clear_handle_cache()
        self.addCleanup(youtube_api.clear_handle_cache)
        with SessionLocal() as db:
            add_registration(db, PLATFORM_YOUTUBE, youtube_id(0))
            add_registration(db, PLATFORM_YOUTUBE, youtube_id(1))
            add_registration(db, PLATFORM_YOUTUBE, youtube_id(0), OTHER_GUILD)  # 別サーバーの登録
            b = Broadcaster(platform=PLATFORM_TWITCH, channel_id="555", name="Streamer",
                            twitch_login="streamer")
            db.add(b)
            db.flush()
            db.add(Subscription(platform=PLATFORM_TWITCH, discord_guild_id=GUILD,
                                discord_channel_id=DISCORD_CHANNEL, youtube_channel_id="555"))
            db.commit()

    def test_remove_youtube_by_url(self):
        with patch("youtube_api.requests.get", side_effect=AssertionError("クオータを使ってはいけない")):
            result = unregister_channel(PLATFORM_YOUTUBE,
                                        "https://www.youtube.com/channel/" + youtube_id(0), GUILD)
        self.assertEqual(result.channel_id, youtube_id(0))
        self.assertEqual(result.remaining, 2)  # YouTube1件 + Twitch1件
        with SessionLocal() as db:
            self.assertIsNone(db.query(Subscription).filter(
                Subscription.discord_guild_id == GUILD,
                Subscription.youtube_channel_id == youtube_id(0)).first())
            # 他サーバーの登録と配信者行は残す
            self.assertIsNotNone(db.query(Subscription).filter(
                Subscription.discord_guild_id == OTHER_GUILD,
                Subscription.youtube_channel_id == youtube_id(0)).first())
            self.assertIsNotNone(db.query(Broadcaster).filter(
                Broadcaster.channel_id == youtube_id(0)).first())

    def test_remove_twitch_by_login_and_url(self):
        result = unregister_channel(PLATFORM_TWITCH, "https://www.twitch.tv/streamer", GUILD)
        self.assertEqual((result.channel_id, result.login, result.name), ("555", "streamer", "Streamer"))
        with self.assertRaises(RegistrationError) as ctx:  # 2回目は登録が無い
            unregister_channel(PLATFORM_TWITCH, "streamer", GUILD)
        self.assertEqual(ctx.exception.code, "not_registered")
        self.assertEqual(ctx.exception.http_status, 404)

    def test_remove_frees_a_slot(self):
        # setUpの3件(YouTube2 + Twitch1)に17件足して合計20にする
        fill_guild(17, start=2)
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 20)
            with self.assertRaises(RegistrationError):
                check_guild_capacity(db, GUILD, PLATFORM_YOUTUBE, youtube_id(99))
        unregister_channel(PLATFORM_YOUTUBE, youtube_id(1), GUILD)
        with SessionLocal() as db:
            self.assertEqual(count_guild_channels(db, GUILD), 19)
            check_guild_capacity(db, GUILD, PLATFORM_YOUTUBE, youtube_id(99))  # 追加できるようになる

    def test_remove_not_registered_channel(self):
        with self.assertRaises(RegistrationError) as ctx:
            unregister_channel(PLATFORM_YOUTUBE, youtube_id(77), GUILD)
        self.assertEqual(ctx.exception.code, "not_registered")

    def test_remove_rejects_unresolvable_input_without_api_call(self):
        """未知のハンドルURLはAPIを呼ばずに断り、ID指定/候補選択を案内する。"""
        with patch("youtube_api.requests.get", side_effect=AssertionError("クオータを使ってはいけない")):
            with self.assertRaises(RegistrationError) as ctx:
                unregister_channel(PLATFORM_YOUTUBE, "https://www.youtube.com/@unknownhandle", GUILD)
        self.assertEqual(ctx.exception.code, "invalid_input")

    def test_remove_outside_guild(self):
        with self.assertRaises(RegistrationError):
            unregister_channel(PLATFORM_YOUTUBE, youtube_id(0), None)

    def test_listing_for_autocomplete(self):
        with SessionLocal() as db:
            youtube_rows = list_guild_registrations(db, GUILD, PLATFORM_YOUTUBE)
            twitch_rows = list_guild_registrations(db, GUILD, PLATFORM_TWITCH)
            other_rows = list_guild_registrations(db, OTHER_GUILD, PLATFORM_TWITCH)
        self.assertEqual([r[0] for r in youtube_rows], [youtube_id(0), youtube_id(1)])
        self.assertEqual(twitch_rows, [("555", "Streamer")])  # IDではなくチャンネル名を出す
        self.assertEqual(other_rows, [])
        self.assertTrue(all(len(label) <= 100 for _, label in youtube_rows + twitch_rows))


if __name__ == "__main__":
    unittest.main()
