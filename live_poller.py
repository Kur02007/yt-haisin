import logging
import re
from typing import Optional

import requests

from config import Config

logger = logging.getLogger(__name__)

# チャンネルが現在ライブ配信中なら、このURLはそのままウォッチページとして
# 200を返す(内部的にYouTube側で内容がすり替わる)。PubSubHubbubのフィード更新を
# 待たずに済むため、ライブ「開始」の検知だけはこちらの方が圧倒的に速い。
LIVE_URL_TEMPLATE = "https://www.youtube.com/channel/{channel_id}/live"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.8",
}

# ページ内の埋め込みJSONから「本当に配信中か」「動画IDは何か」を拾うための簡易パターン。
# 公式APIではないため、YouTube側のマークアップ変更で壊れる可能性がある。
# その場合はこのファイル内のパターンだけを直せばよい(他モジュールには影響しない)。
# 実測: 配信中でないチャンネルでは "isLive":true は出現せず、canonicalも
# /channel/{id} のまま(watchページにならない)。配信中は両方の条件が揃う。
_IS_LIVE_RE = re.compile(r'"isLive"\s*:\s*true')
_CANONICAL_WATCH_RE = re.compile(
    r'<link rel="canonical" href="https://www\.youtube\.com/watch\?v=([\w-]{11})"'
)


def get_current_live_video_id(channel_id: str) -> Optional[str]:
    """チャンネルが今まさにライブ配信中なら動画IDを、そうでなければNoneを返す。

    配信予約(upcoming)やプレミア公開待ちはisLiveが立たないため対象外
    (それらはPubSubHubbub側の通知やclassify_broadcastで扱う想定)。
    """
    url = LIVE_URL_TEMPLATE.format(channel_id=channel_id)
    try:
        response = requests.get(
            url,
            headers=_HEADERS,
            timeout=Config.LIVE_POLL_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
    except requests.exceptions.RequestException as e:
        logger.warning(f"[live_poller] channel_id={channel_id} へのアクセスに失敗しました: {e}")
        return None

    if response.status_code != 200:
        return None

    html = response.text
    if not _IS_LIVE_RE.search(html):
        return None

    # canonicalがwatchページのときだけ採用する。ページ内の最初のvideoIdで代用すると、
    # おすすめ欄など他チャンネルの動画を拾って誤通知になる(2026-10-07に実害あり)。
    match = _CANONICAL_WATCH_RE.search(html)
    return match.group(1) if match else None
