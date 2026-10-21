"""ハンドルURL→チャンネルID変換のオフラインテスト(外部通信なし・requestsはモック)。"""
import os
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test")
os.environ.setdefault("YOUTUBE_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("CALLBACK_URL", "https://example.invalid/webhook/youtube")
import unittest
from unittest.mock import Mock, patch

import youtube_api
from channel_registration import RegistrationError, resolve_youtube_channel_id
from admin_schemas import ChannelCreateRequest

CHANNEL_ID = "UC0topOTAhXlxevzudmI8w7g"
CHANNEL_URL = "https://www.youtube.com/channel/" + CHANNEL_ID
HANDLE_URL = "https://www.youtube.com/@shizuki_mana"
# 日本語ハンドル。ブラウザからコピーするとパーセントエンコードされた形になる。
JP_HANDLE = "ちわのまこ"
JP_HANDLE_URL = "https://www.youtube.com/@" + JP_HANDLE
JP_HANDLE_URL_ENCODED = "https://www.youtube.com/@%E3%81%A1%E3%82%8F%E3%81%AE%E3%81%BE%E3%81%93"


def response(status=200, payload=None):
    resp = Mock()
    resp.status_code = status
    resp.json.return_value = payload if payload is not None else {}
    resp.text = ""
    return resp


CHANNEL_NAME = "紫月まな"


def ok_response(name=CHANNEL_NAME):
    return response(200, {"items": [{"kind": "youtube#channel", "id": CHANNEL_ID,
                                     "snippet": {"title": name}}]})


class HandleParsingTest(unittest.TestCase):
    """入力の形だけで判定する部分(外部アクセスなし)。"""

    def test_handle_forms(self):
        for value in (HANDLE_URL, "https://youtube.com/@shizuki_mana",
                      "http://www.youtube.com/@shizuki_mana",  # httpのコピペも受け付ける
                      "http://youtube.com/@shizuki_mana",
                      "http://www.youtube.com/@shizuki_mana?si=abc",
                      "HTTP://WWW.YouTube.com/@shizuki_mana",
                      "https://m.youtube.com/@shizuki_mana/videos",
                      "https://music.youtube.com/@shizuki_mana",
                      "www.youtube.com/@shizuki_mana?si=abc", "youtube.com/@shizuki_mana",
                      " https://www.youtube.com/@shizuki_mana ",  # 前後の空白
                      "@shizuki_mana"):
            with self.subTest(value=value):
                self.assertEqual(youtube_api.parse_youtube_handle(value), "shizuki_mana")

    def test_percent_encoded_and_non_ascii_handles(self):
        """日本語ハンドルは、そのままでもパーセントエンコードされていても同じ名前になる。"""
        for value in (JP_HANDLE_URL, JP_HANDLE_URL_ENCODED, "@" + JP_HANDLE,
                      JP_HANDLE_URL_ENCODED + "/videos",
                      JP_HANDLE_URL_ENCODED.replace("https://", "http://") + "?si=abc"):
            with self.subTest(value=value):
                self.assertEqual(youtube_api.parse_youtube_handle(value), JP_HANDLE)

    def test_broken_or_out_of_spec_encoding(self):
        for value in ("https://www.youtube.com/@%E3%81",      # 壊れたエンコード
                      "https://www.youtube.com/@a%20b",       # デコードすると空白が入る
                      "https://www.youtube.com/@%E3%81%82"):  # デコードして1文字(仕様は3文字以上)
            with self.subTest(value=value):
                self.assertIsNone(youtube_api.parse_youtube_handle(value))

    def test_not_a_handle(self):
        for value in (None, "", CHANNEL_URL, CHANNEL_ID,
                      "https://www.youtube.com/c/SomeName",
                      "https://www.youtube.com/user/SomeName",
                      "https://example.com/@shizuki_mana", "@ab"):
            with self.subTest(value=value):
                self.assertIsNone(youtube_api.parse_youtube_handle(value))

    def test_channel_id_parsing(self):
        self.assertEqual(youtube_api.parse_youtube_channel_id(CHANNEL_URL), CHANNEL_ID)
        self.assertIsNone(youtube_api.parse_youtube_channel_id(HANDLE_URL))


class ResolveTest(unittest.TestCase):
    def setUp(self):
        youtube_api.clear_handle_cache()
        self.api_key = patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key")
        self.api_key.start()
        self.addCleanup(self.api_key.stop)

    def test_handle_url_becomes_channel_url(self):
        with patch("youtube_api.requests.get", return_value=ok_response()) as get:
            self.assertEqual(youtube_api.resolve_channel_url(HANDLE_URL), CHANNEL_URL)
        self.assertEqual(get.call_count, 1)  # 1チャンネル＝クオータ1
        self.assertEqual(get.call_args.kwargs["params"]["forHandle"], "@shizuki_mana")
        # snippet を足してもクオータは1のまま(チャンネル名を同時に受け取るため)
        self.assertEqual(get.call_args.kwargs["params"]["part"], "id,snippet")

    def test_http_handle_url_becomes_channel_url(self):
        with patch("youtube_api.requests.get", return_value=ok_response()) as get:
            self.assertEqual(
                youtube_api.resolve_channel_url("http://www.youtube.com/@shizuki_mana"), CHANNEL_URL)
        self.assertEqual(get.call_count, 1)

    def test_encoded_japanese_handle_becomes_channel_url(self):
        with patch("youtube_api.requests.get", return_value=ok_response()) as get:
            self.assertEqual(youtube_api.resolve_channel_url(JP_HANDLE_URL_ENCODED), CHANNEL_URL)
        # APIにはデコードした状態で渡す(requestsが送信時に再エンコードする)
        self.assertEqual(get.call_args.kwargs["params"]["forHandle"], "@" + JP_HANDLE)
        # エンコードの有無が違っても同じハンドル＝追加のクオータを使わない
        with patch("youtube_api.requests.get", side_effect=AssertionError("外部問い合わせ禁止")):
            self.assertEqual(youtube_api.resolve_channel_id(JP_HANDLE_URL), CHANNEL_ID)
        self.assertEqual(get.call_count, 1)

    def test_resolve_returns_channel_name(self):
        """ハンドル変換と同じ1クオータでチャンネル名も分かる。"""
        with patch("youtube_api.requests.get", return_value=ok_response()) as get:
            resolved = youtube_api.resolve_channel(HANDLE_URL)
        self.assertEqual((resolved.channel_id, resolved.name), (CHANNEL_ID, CHANNEL_NAME))
        self.assertEqual(get.call_count, 1)
        # キャッシュからも名前を返す(追加のクオータ無し)
        with patch("youtube_api.requests.get", side_effect=AssertionError("外部問い合わせ禁止")):
            self.assertEqual(youtube_api.resolve_channel(HANDLE_URL).name, CHANNEL_NAME)

    def test_channel_id_input_has_no_name(self):
        """チャンネルIDで指定された場合、名前は分からない(通知が届いた時点で埋まる)。"""
        with patch("youtube_api.requests.get", side_effect=AssertionError("外部問い合わせ禁止")):
            self.assertEqual(youtube_api.resolve_channel(CHANNEL_URL),
                             (CHANNEL_ID, None))

    def test_missing_snippet_title_is_tolerated(self):
        with patch("youtube_api.requests.get",
                   return_value=response(200, {"items": [{"id": CHANNEL_ID}]})):
            self.assertEqual(youtube_api.resolve_channel(HANDLE_URL), (CHANNEL_ID, None))

    def test_channel_id_needs_no_request(self):
        with patch("youtube_api.requests.get", side_effect=AssertionError("外部問い合わせ禁止")):
            self.assertEqual(youtube_api.resolve_channel_url(CHANNEL_URL), CHANNEL_URL)
            self.assertEqual(youtube_api.resolve_channel_url(CHANNEL_ID), CHANNEL_URL)

    def test_cache_suppresses_second_request(self):
        with patch("youtube_api.requests.get", return_value=ok_response()) as get:
            youtube_api.resolve_channel_id(HANDLE_URL)
            youtube_api.resolve_channel_id("@Shizuki_Mana")  # 大文字小文字違いも同じ扱い
            # http/httpsやサブドメインの違いも同じハンドル＝追加のクオータを使わない
            youtube_api.resolve_channel_id("http://www.youtube.com/@shizuki_mana")
            youtube_api.resolve_channel_id("https://m.youtube.com/@shizuki_mana/videos")
        self.assertEqual(get.call_count, 1)

    def test_not_found_is_cached_and_not_transient(self):
        with patch("youtube_api.requests.get", return_value=response(200, {"items": []})) as get:
            for _ in range(2):
                with self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                    youtube_api.resolve_channel_id(HANDLE_URL)
                self.assertEqual(ctx.exception.code, "not_found")
                self.assertFalse(ctx.exception.transient)
        self.assertEqual(get.call_count, 1)

    def test_http_404_is_not_found(self):
        with patch("youtube_api.requests.get", return_value=response(404)):
            with self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                youtube_api.resolve_channel_id(HANDLE_URL)
        self.assertEqual(ctx.exception.code, "not_found")

    def test_invalid_input(self):
        for value in ("https://example.com/@name", "ただの文字列", ""):
            with self.subTest(value=value), self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                youtube_api.resolve_channel_id(value)
            self.assertEqual(ctx.exception.code, "invalid_input")
            self.assertFalse(ctx.exception.transient)

    def test_transient_failures(self):
        cases = {403: "quota_exceeded", 429: "api_http_429", 500: "api_http_500"}
        for status, code in cases.items():
            with self.subTest(status=status):
                youtube_api.clear_handle_cache()
                with patch("youtube_api.requests.get", return_value=response(status)):
                    with self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                        youtube_api.resolve_channel_id(HANDLE_URL)
                self.assertEqual(ctx.exception.code, code)
                self.assertTrue(ctx.exception.transient)

    def test_transient_failure_is_not_cached(self):
        with patch("youtube_api.requests.get", return_value=response(500)):
            with self.assertRaises(youtube_api.YouTubeResolveError):
                youtube_api.resolve_channel_id(HANDLE_URL)
        with patch("youtube_api.requests.get", return_value=ok_response()):
            self.assertEqual(youtube_api.resolve_channel_id(HANDLE_URL), CHANNEL_ID)

    def test_network_error_is_transient(self):
        with patch("youtube_api.requests.get", side_effect=youtube_api.requests.Timeout()):
            with self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                youtube_api.resolve_channel_id(HANDLE_URL)
        self.assertEqual(ctx.exception.code, "network_timeout")
        self.assertTrue(ctx.exception.transient)

    def test_missing_api_key_only_affects_handles(self):
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", None):
            with self.assertRaises(youtube_api.YouTubeResolveError) as ctx:
                youtube_api.resolve_channel_id(HANDLE_URL)
            self.assertEqual(ctx.exception.code, "not_configured")
            self.assertFalse(ctx.exception.transient)
            self.assertEqual(youtube_api.resolve_channel_id(CHANNEL_URL), CHANNEL_ID)


class RegistrationLayerTest(unittest.TestCase):
    """登録処理が利用者向けメッセージ・HTTPステータスに翻訳できているか。"""

    def setUp(self):
        youtube_api.clear_handle_cache()

    def test_resolve_for_registration(self):
        with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                patch("youtube_api.requests.get", return_value=ok_response()):
            self.assertEqual(resolve_youtube_channel_id(HANDLE_URL), CHANNEL_ID)

    def test_error_mapping(self):
        cases = [(response(200, {"items": []}), "not_found", 404),
                 (response(500), "api_http_500", 502)]
        for resp, code, status in cases:
            with self.subTest(code=code):
                youtube_api.clear_handle_cache()
                with patch.object(youtube_api.Config, "YOUTUBE_API_KEY", "test-key"), \
                        patch("youtube_api.requests.get", return_value=resp):
                    with self.assertRaises(RegistrationError) as ctx:
                        resolve_youtube_channel_id(HANDLE_URL)
                self.assertEqual(ctx.exception.code, code)
                self.assertEqual(ctx.exception.http_status, status)
                self.assertTrue(ctx.exception.message)

    def test_invalid_input_mapping(self):
        with self.assertRaises(RegistrationError) as ctx:
            resolve_youtube_channel_id("https://example.com/@name")
        self.assertEqual(ctx.exception.http_status, 422)

    def test_admin_request_accepts_handle_without_network(self):
        with patch("youtube_api.requests.get", side_effect=AssertionError("外部問い合わせ禁止")):
            req = ChannelCreateRequest(platform="youtube", identifier=HANDLE_URL)
            self.assertEqual(req.identifier, HANDLE_URL)
            ChannelCreateRequest(platform="youtube", identifier=CHANNEL_URL)
            with self.assertRaises(ValueError):
                ChannelCreateRequest(platform="youtube", identifier="https://example.com/@name")


if __name__ == "__main__":
    unittest.main()
