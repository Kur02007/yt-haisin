import logging
import re
import threading
import time
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

import requests

from config import Config

logger = logging.getLogger(__name__)

YOUTUBE_VIDEOS_API_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNELS_API_URL = "https://www.googleapis.com/youtube/v3/channels"
JST = timezone(timedelta(hours=9))

# タイトルに含まれていれば「メンバー限定」とみなすキーワード。
# 標準のRSS/Atom通知やYouTube Data APIには判別用の公式フィールドが存在しないため、
# 配信者が自主的にタイトルへ付けているラベルを頼りにするベストエフォートの判定。
MEMBERS_ONLY_KEYWORDS = [
    "メンバー限定",
    "メン限",
    "members only",
    "members-only",
    "member only",
]

# タイトルに含まれていれば「プレミア公開」とみなすキーワード(duration判定より優先する強いシグナル)
PREMIERE_KEYWORDS = [
    "プレミア公開",
    "プレミア配信",
    "premiere",
]


def fetch_video_details(video_id):
    """videos.list を1回呼び出す(動画IDが1件でもクオータは1)。
    失敗時はNoneを返し、呼び出し元は判定不能として安全側にフォールバックする。"""
    if not Config.YOUTUBE_API_KEY:
        return None

    params = {
        "part": "snippet,contentDetails,liveStreamingDetails,statistics",
        "id": video_id,
        "key": Config.YOUTUBE_API_KEY,
    }
    try:
        resp = requests.get(YOUTUBE_VIDEOS_API_URL, params=params, timeout=10)
        if resp.status_code != 200:
            logger.error(
                f"[system_error:youtube_api] videos.list失敗 video_id={video_id} "
                f"status={resp.status_code} body={resp.text[:200]}"
            )
            return None

        items = resp.json().get("items", [])
        if not items:
            return None
        return items[0]
    except requests.exceptions.RequestException as e:
        logger.error(
            f"[system_error:youtube_api] videos.listへの接続に失敗しました "
            f"video_id={video_id}: {e}"
        )
        return None


def to_unix_timestamp(iso_utc_str):
    """'2026-08-11T10:00:00Z' のようなUTC文字列をUNIX秒にする(Discordの <t:…> 記法用)。"""
    if not iso_utc_str:
        return None
    try:
        return int(datetime.fromisoformat(iso_utc_str.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def format_jst(iso_utc_str):
    """'2026-08-11T10:00:00Z' のようなUTC文字列を '8月11日 19:00' (JST) に変換する"""
    if not iso_utc_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_utc_str.replace("Z", "+00:00"))
        dt_jst = dt.astimezone(JST)
        return dt_jst.strftime("%-m月%-d日 %H:%M")
    except ValueError:
        return None


def is_probably_members_only(title):
    """タイトル文字列からメンバー限定を推測する(ベストエフォート、確定情報ではない)"""
    lowered = title.lower()
    return any(keyword.lower() in lowered for keyword in MEMBERS_ONLY_KEYWORDS)


def _title_declares_premiere(title):
    lowered = title.lower()
    return any(keyword.lower() in lowered for keyword in PREMIERE_KEYWORDS)


def _is_view_count_hidden(api_details):
    """statistics.viewCount キー自体が欠落しているか(未文書化のメンバー限定シグナル)。

    実測: 確認済みメンバー限定動画(配信済みアーカイブ)は5/5でviewCountキーが完全欠落し、
    公開動画は5/5で必ず数値付きだった。ただし配信開始前(upcoming)の動画はメンバー限定でも
    viewCount:'0'として値付きで存在しており、この判定はupcoming/live状態には使えない。
    公開直後でまだ再生数が付いていない"通常動画"でどう振る舞うかは未検証のため、
    あくまでタイトルキーワードを補強する副次シグナルとして扱う。"""
    statistics = api_details.get("statistics")
    if statistics is None:
        return False
    return "viewCount" not in statistics


def classify_broadcast(video_id, title):
    """videos.list の結果から、通知メッセージの出し分けに使う配信種別を判定する。

    判定材料(YouTube公式Data APIの範囲内、非公式スクレイピングは使わない):
    - snippet.liveBroadcastContent: upcoming/live/none
    - contentDetails.duration: 予定/配信中のライブは "P0D"(尺未定)になる一方、
      プレミア公開は収録済み動画のため実際の尺が既に入る、という実測に基づくヒューリスティック。
      ※このヒューリスティックはライブ側("P0D")は実データで確認済みだが、プレミア側は
      「配信前(upcoming)状態」のサンプルを確認できておらず、コミュニティ情報からの推測を含む。
      そのためタイトルに「プレミア公開」等の明示的な文言がある場合はそちらを優先する。
    - statistics.viewCount の欠落: 通常動画(broadcast_state="video")限定の補助シグナル。
      タイトルにメンバー限定を示す語が無くても、これが欠落していれば追加でメンバー限定と判定する
      (詳細は _is_view_count_hidden のdocstring参照)。

    API呼び出しが失敗した場合は、判定不能として元の挙動(通常配信扱い)にフォールバックする。
    """
    title_says_members_only = is_probably_members_only(title)
    result = {
        "broadcast_state": "live",  # フォールバック時のデフォルト
        "scheduled_time_jst": None,
        "scheduled_start_unix": None,  # 日本語以外の通知で <t:…> 記法に使う
        "is_members_only": title_says_members_only,
        "raw": None,
    }

    api_details = fetch_video_details(video_id)
    if not api_details:
        return result

    result["raw"] = api_details
    snippet = api_details.get("snippet", {}) or {}
    content_details = api_details.get("contentDetails", {}) or {}
    live_details = api_details.get("liveStreamingDetails", {}) or {}

    live_broadcast_content = snippet.get("liveBroadcastContent", "none")
    duration = content_details.get("duration")
    scheduled_start = live_details.get("scheduledStartTime")

    is_scheduled_or_live = live_broadcast_content in ("upcoming", "live")
    has_known_duration = duration not in (None, "P0D")
    is_probably_premiere = _title_declares_premiere(title) or (
        is_scheduled_or_live and has_known_duration
    )

    if is_scheduled_or_live:
        if is_probably_premiere:
            state = "premiere_live" if live_broadcast_content == "live" else "premiere_upcoming"
        else:
            state = "live" if live_broadcast_content == "live" else "upcoming"
    else:
        state = "video"

    view_count_hidden = state == "video" and _is_view_count_hidden(api_details)
    if view_count_hidden:
        result["is_members_only"] = True

    result["broadcast_state"] = state
    result["scheduled_time_jst"] = format_jst(scheduled_start)
    result["scheduled_start_unix"] = to_unix_timestamp(scheduled_start)

    logger.info(
        f"[youtube_api] video_id={video_id} liveBroadcastContent={live_broadcast_content} "
        f"duration={duration} view_count_hidden={view_count_hidden} "
        f"title_says_members_only={title_says_members_only} "
        f"-> broadcast_state={state} members_only={result['is_members_only']}"
    )

    return result


# --- ハンドルURL(@name) → チャンネルID(UC...) の変換 ---------------------------
# 公式の YouTube Data API v3 (channels.list?forHandle=) だけを使う。
# チャンネルページのHTMLを解釈する方式は利用規約・仕様変更の面で不安定なため採らない。
# クオータは channels.list 1回につき1。同じハンドルの再変換はキャッシュで抑える。

CHANNEL_ID_URL_TEMPLATE = "https://www.youtube.com/channel/%s"

_CHANNEL_ID_RE = re.compile(r"(UC[\w-]{22})")
# URL形式(www/m/music などのサブドメインと、末尾の /videos・?si=... を許容)と裸の@nameの2通り。
# ハンドル部分はここでは大まかに切り出すだけにして、中身の検査は _normalize_handle で行う。
_HANDLE_URL_RE = re.compile(
    r"^(?:https?://)?(?:[\w-]+\.)*youtube\.com/@([^/?#\s]{1,120})(?:[/?#].*)?$",
    re.IGNORECASE,
)
_BARE_HANDLE_RE = re.compile(r"^@([^/?#\s]{1,120})$")
# ハンドルはYouTubeの仕様で3〜30文字。英数字・ドット・アンダースコア・ハイフンのほか、
# 日本語など非ASCIIの文字も実在する(\w はUnicodeの文字・数字・アンダースコアを含む)。
_HANDLE_BODY_RE = re.compile(r"^[\w.-]{3,30}$", re.UNICODE)

# 成功した変換はほぼ不変なので長め、「見つからない」は改名・新規作成を拾えるよう短めに保持する。
HANDLE_CACHE_TTL_SECONDS = 24 * 60 * 60
HANDLE_NEGATIVE_CACHE_TTL_SECONDS = 10 * 60

# 解決結果。name はハンドル経由で分かったチャンネル名(不明ならNone)。
ResolvedChannel = namedtuple("ResolvedChannel", "channel_id name")

_handle_cache = {}  # handle(小文字) -> (期限のmonotonic時刻, ResolvedChannel または None)
_handle_cache_lock = threading.Lock()


class YouTubeResolveError(Exception):
    """ハンドル変換の失敗。code は固定文字列、transient=True は時間をおけば通る見込みがあるもの。

    code: invalid_input / not_configured / not_found / quota_exceeded /
          api_http_<status> / network_timeout / network_connection / network_request /
          api_invalid_response
    """

    def __init__(self, code, transient=False):
        self.code = code
        self.transient = transient
        super().__init__(code)


def parse_youtube_channel_id(value):
    """UC...形式のチャンネルIDを取り出す(URL・ID文字列のどちらでも可)。見つからなければNone。"""
    match = _CHANNEL_ID_RE.search(value or "")
    return match.group(1) if match else None


def _normalize_handle(raw):
    """URLから切り出したハンドル部分をデコードし、妥当なら返す(不正ならNone)。

    ブラウザのアドレスバーからコピーすると日本語ハンドルは
    %E3%81%A1... のようにパーセントエンコードされるため、ここで元に戻す。
    """
    try:
        handle = unquote(raw, errors="strict")
    except (UnicodeDecodeError, ValueError):
        return None
    return handle if _HANDLE_BODY_RE.match(handle) else None


def parse_youtube_handle(value):
    """ハンドル(@name / ハンドルURL)から '@' を除いたハンドル名を返す。該当しなければNone。

    パーセントエンコードされたURLはデコードしたハンドル名を返す。外部アクセスはしない。
    """
    text = (value or "").strip()
    if not text:
        return None
    # チャンネルID URLや /c/、/user/ など別形式を誤ってハンドル扱いしない
    if _CHANNEL_ID_RE.search(text):
        return None
    match = _HANDLE_URL_RE.match(text) or _BARE_HANDLE_RE.match(text)
    return _normalize_handle(match.group(1)) if match else None


def _cache_get(handle):
    key = handle.lower()
    with _handle_cache_lock:
        entry = _handle_cache.get(key)
        if entry is None:
            return False, None
        expires_at, channel_id = entry
        if time.monotonic() >= expires_at:
            del _handle_cache[key]
            return False, None
        return True, channel_id


def _cache_put(handle, resolved):
    ttl = HANDLE_CACHE_TTL_SECONDS if resolved else HANDLE_NEGATIVE_CACHE_TTL_SECONDS
    with _handle_cache_lock:
        _handle_cache[handle.lower()] = (time.monotonic() + ttl, resolved)


def cached_channel_id(handle):
    """キャッシュ済みのハンドルのチャンネルIDを返す(外部アクセスなし)。未知・未解決ならNone。"""
    if not handle:
        return None
    cached, resolved = _cache_get(handle)
    return resolved.channel_id if cached and resolved else None


def clear_handle_cache():
    """テストや手動リセット用。キャッシュを空にする。"""
    with _handle_cache_lock:
        _handle_cache.clear()


def _fetch_channel_by_handle(handle):
    """channels.list を1回呼ぶ(クオータ1)。見つからなければNone、失敗は例外。

    part に snippet を足してもクオータは1のままなので、
    表示用のチャンネル名(snippet.title)もここで一緒に受け取る。"""
    params = {"part": "id,snippet", "forHandle": "@" + handle, "key": Config.YOUTUBE_API_KEY}
    try:
        resp = requests.get(YOUTUBE_CHANNELS_API_URL, params=params, timeout=10)
    except requests.Timeout:
        raise YouTubeResolveError("network_timeout", True) from None
    except requests.ConnectionError:
        raise YouTubeResolveError("network_connection", True) from None
    except requests.RequestException:
        raise YouTubeResolveError("network_request", True) from None

    if resp.status_code == 404:
        # forHandle に一致するチャンネルが無い場合、200+items空 のほかに404が返ることがある
        return None
    if resp.status_code != 200:
        # 429(レート制限)と5xxは時間をおけば通る見込みがある。
        # 403は多くがクオータ超過(日次でリセット)なので一時的失敗として扱う。
        transient = resp.status_code in (403, 429) or 500 <= resp.status_code <= 599
        code = "quota_exceeded" if resp.status_code == 403 else "api_http_%d" % resp.status_code
        logger.error(
            f"[system_error:youtube_api] channels.list失敗 handle=@{handle} "
            f"status={resp.status_code} body={resp.text[:200]}"
        )
        raise YouTubeResolveError(code, transient)

    try:
        items = resp.json().get("items", [])
    except ValueError:
        raise YouTubeResolveError("api_invalid_response", True) from None
    if not items:
        return None
    channel_id = parse_youtube_channel_id(items[0].get("id", ""))
    if not channel_id:
        raise YouTubeResolveError("api_invalid_response", True)
    # チャンネル名の前後に空白が入っていることがあるので落としてから使う
    title = ((items[0].get("snippet") or {}).get("title") or "").strip() or None
    return ResolvedChannel(channel_id, title)


def resolve_channel(value):
    """URL・ID・ハンドルのいずれかから ResolvedChannel(channel_id, name) を返す。

    - 既にチャンネルID(URL含む)なら外部問い合わせなしで返す(name は不明なのでNone)
    - ハンドルなら channels.list で変換する(クオータ1。結果はキャッシュする)
    失敗時は YouTubeResolveError を送出する。
    """
    channel_id = parse_youtube_channel_id(value)
    if channel_id:
        return ResolvedChannel(channel_id, None)

    handle = parse_youtube_handle(value)
    if not handle:
        raise YouTubeResolveError("invalid_input")

    cached, resolved = _cache_get(handle)
    if cached:
        if resolved:
            return resolved
        raise YouTubeResolveError("not_found")

    if not Config.YOUTUBE_API_KEY:
        # キー未設定はハンドル変換そのものが使えない(設定の問題であり再試行しても直らない)
        raise YouTubeResolveError("not_configured")

    resolved = _fetch_channel_by_handle(handle)
    _cache_put(handle, resolved)
    logger.info(f"[youtube_api] handle=@{handle} -> "
                f"channel_id={resolved.channel_id if resolved else 'not_found'}")
    if not resolved:
        raise YouTubeResolveError("not_found")
    return resolved


def resolve_channel_id(value):
    """URL・ID・ハンドルのいずれかからチャンネルID(UC...)を返す。"""
    return resolve_channel(value).channel_id


def resolve_channel_url(value):
    """URL・ID・ハンドルを https://www.youtube.com/channel/UC... に正規化して返す。"""
    return CHANNEL_ID_URL_TEMPLATE % resolve_channel_id(value)
