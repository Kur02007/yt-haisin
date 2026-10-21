"""Twitch Helix / EventSub の最小クライアント(Client Credentials Flow のアプリトークン)。

Client ID / Client Secret / Webhook secret / アクセストークンはログ・例外に出さない。
失敗は TwitchAPIError の固定コード(例: "users_http_500", "network_timeout")だけで伝える。
"""
import logging
import threading
import time

import requests

from config import Config

logger = logging.getLogger(__name__)

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
HELIX_URL = "https://api.twitch.tv/helix"
STREAM_ONLINE = "stream.online"
STREAM_OFFLINE = "stream.offline"
# 配信開始で通知し、配信終了でリンクをアーカイブへ差し替えるため、両方を購読する。
EVENT_TYPES = (STREAM_ONLINE, STREAM_OFFLINE)
# トークン期限のこの秒数前から新しいトークンに切り替える。
TOKEN_REFRESH_MARGIN_SECONDS = 300

_token_lock = threading.Lock()
_token = {"value": None, "expires_at": 0.0}


class TwitchAPIError(Exception):
    def __init__(self, code, transient=False, status=None):
        self.code = code
        self.transient = transient
        self.status = status
        super().__init__(code)


def _request(method, url, **kwargs):
    # 認証情報をリダイレクト先へ送らない。例外文字列(URL等)は捨てて固定コードにする。
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    try:
        return requests.request(method, url, timeout=Config.TWITCH_REQUEST_TIMEOUT_SECONDS,
                                allow_redirects=False, **kwargs)
    except requests.Timeout:
        raise TwitchAPIError("network_timeout", True) from None
    except requests.ConnectionError:
        raise TwitchAPIError("network_connection", True) from None
    except requests.RequestException:
        raise TwitchAPIError("network_request", True) from None


def _http_error(response, operation):
    code = response.status_code
    # 429(レート制限)と5xxは時間をおけば通る見込みがある。
    return TwitchAPIError("%s_http_%d" % (operation, code), code == 429 or 500 <= code <= 599, code)


def _get_app_token(force_refresh=False):
    with _token_lock:
        if not force_refresh and _token["value"] and time.time() < _token["expires_at"]:
            return _token["value"]
        response = _request("POST", TOKEN_URL, data={
            "client_id": Config.TWITCH_CLIENT_ID,
            "client_secret": Config.TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
        })
        try:
            if response.status_code != 200:
                raise _http_error(response, "token")
            body = response.json()
            value = body["access_token"]
            expires_in = float(body.get("expires_in", 3600))
        except (ValueError, KeyError, TypeError):
            raise TwitchAPIError("token_invalid_response", True) from None
        finally:
            response.close()
        _token["value"] = value
        _token["expires_at"] = time.time() + max(60.0, expires_in - TOKEN_REFRESH_MARGIN_SECONDS)
        logger.info("Twitch app access token refreshed count=1")
        return value


def _helix(method, path, operation, params=None, json=None):
    """Helix呼び出し。401(トークン失効・取り消し)なら1回だけトークンを取り直して再試行する。"""
    error = Config.twitch_config_error()
    if error:
        raise TwitchAPIError(error)
    for attempt in range(2):
        token = _get_app_token(force_refresh=attempt > 0)
        response = _request(method, HELIX_URL + path, params=params, json=json, headers={
            "Client-Id": Config.TWITCH_CLIENT_ID,
            "Authorization": "Bearer " + token,
        })
        try:
            if response.status_code == 401 and attempt == 0:
                continue
            if response.status_code >= 300:
                raise _http_error(response, operation)
            if response.status_code == 204:
                return None
            try:
                return response.json()
            except ValueError:
                raise TwitchAPIError(operation + "_invalid_response", True) from None
        finally:
            response.close()
    raise TwitchAPIError(operation + "_http_401", False, 401)


def get_user_by_login(login):
    """ログイン名からユーザーを引く。存在しなければNone。"""
    body = _helix("GET", "/users", "users", params={"login": login.lower()})
    data = (body or {}).get("data") or []
    return data[0] if data else None


def get_channel_info(broadcaster_id):
    """現在の配信タイトル等(Get Channel Information)。取得できなければNone。"""
    body = _helix("GET", "/channels", "channels", params={"broadcaster_id": broadcaster_id})
    data = (body or {}).get("data") or []
    return data[0] if data else None


def list_subscriptions(broadcaster_id, event_type=None):
    """このアプリが持つ、指定配信者の購読(全status)を返す。event_typeを指定すると種類で絞る。"""
    # Helixは複数フィルタの同時指定ができないため user_id で絞り、typeはこちらで絞る。
    result, cursor = [], None
    for _ in range(20):
        params = {"user_id": broadcaster_id}
        if cursor:
            params["after"] = cursor
        body = _helix("GET", "/eventsub/subscriptions", "eventsub_list", params=params) or {}
        for sub in body.get("data") or []:
            condition = sub.get("condition") or {}
            if (sub.get("type") in (EVENT_TYPES if event_type is None else (event_type,))
                    and condition.get("broadcaster_user_id") == broadcaster_id):
                result.append(sub)
        cursor = (body.get("pagination") or {}).get("cursor")
        if not cursor:
            break
    return result


def create_subscription(broadcaster_id, event_type):
    """stream.online / stream.offline のWebhook購読を作成する。作成直後のstatusは
    webhook_callback_verification_pending で、/webhook/twitch がchallengeに応答すると有効になる。
    同一条件の購読が既にあれば TwitchAPIError("eventsub_create_http_409")。"""
    body = _helix("POST", "/eventsub/subscriptions", "eventsub_create", json={
        "type": event_type,
        "version": "1",
        "condition": {"broadcaster_user_id": broadcaster_id},
        "transport": {
            "method": "webhook",
            "callback": Config.TWITCH_CALLBACK_URL,
            "secret": Config.TWITCH_WEBHOOK_SECRET,
        },
    })
    data = (body or {}).get("data") or []
    if not data:
        raise TwitchAPIError("eventsub_create_invalid_response", True)
    return data[0]


def get_archive_video(broadcaster_id, stream_id):
    """配信IDに対応するアーカイブ(VOD)を返す。保存設定オフ・生成前・期限切れならNone。
    Helixのvideosは配信IDを持っているため、これで配信と確実に結びつけられる。"""
    body = _helix("GET", "/videos", "videos",
                  params={"user_id": broadcaster_id, "type": "archive", "first": "20"})
    for video in (body or {}).get("data") or []:
        if str(video.get("stream_id") or "") == str(stream_id):
            return video
    return None


def delete_eventsub_subscription(subscription_id):
    try:
        _helix("DELETE", "/eventsub/subscriptions", "eventsub_delete", params={"id": subscription_id})
    except TwitchAPIError as error:
        if error.status != 404:  # 既に消えているなら目的は達成済み
            raise
