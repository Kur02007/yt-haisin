import logging
from datetime import datetime

import requests

from config import Config
from i18n import t

logger = logging.getLogger(__name__)

# send_discord_message() / send_followup_members_only_message() の戻り値。
# 「送れなかった」だけでなく「なぜ送れなかったか」を呼び出し元に伝えるために使う。
SEND_OK = "ok"                            # 送信成功
SEND_CHANNEL_MISSING = "channel_missing"  # 宛先が存在しない(404 / code 10003)＝設定削除の対象
SEND_FAILED = "failed"                    # その他の失敗(権限不足・Discord障害・通信断など)

DISCORD_UNKNOWN_CHANNEL = 10003

DISCORD_API_URL = "https://discord.com/api/v10/channels/{}/messages"
DISCORD_MESSAGE_URL = "https://discord.com/api/v10/channels/{}/messages/{}"
HEADERS = {
    "Authorization": f"Bot {Config.DISCORD_BOT_TOKEN}",
    "Content-Type": "application/json"
}


def build_default_content_text(data, classification, language="ja"):
    """配信種別(通常/プレミア/メンバー限定)に応じて通知文言を出し分ける。
    classification は youtube_api.classify_broadcast() の戻り値。
    API判定に失敗した場合は classification['broadcast_state'] が 'live' になり、
    従来通り「配信を開始しました」の文言にフォールバックする。
    language はサーバーの言語設定(i18n.py)。文言は i18n.MESSAGES の notify_* / kind_*。"""
    author = data['author_name']
    state = classification.get('broadcast_state', 'live')
    scheduled_time = classification.get('scheduled_time_jst')
    members_only = classification.get('is_members_only', False)
    # 日本語以外は「8月11日 19:00」(JST)では読めないため、Discordのタイムスタンプ記法にする。
    # 見る人のタイムゾーン・表示言語で日時が出る。UNIX秒が無いときは JST と明記する。
    if scheduled_time and language != "ja":
        unix = classification.get('scheduled_start_unix')
        scheduled_time = f"<t:{unix}:f>" if unix else f"{scheduled_time} (JST)"

    lock = "🔒" if members_only else ""
    suffix = "_members" if members_only else ""

    def text(key, kind_key):
        return t(language, key, lock=lock, name=author, time=scheduled_time,
                 kind=t(language, kind_key + suffix))

    if state == "premiere_upcoming" and scheduled_time:
        return text("notify_premiere_upcoming", "kind_premiere")
    if state == "premiere_live" or (state == "premiere_upcoming" and not scheduled_time):
        return text("notify_premiere_live", "kind_premiere")
    if state == "upcoming" and scheduled_time:
        return text("notify_upcoming", "kind_live")
    if state == "video":
        return text("notify_video", "kind_video")
    # state == "live"、または判定不能時のフォールバック
    return text("notify_live", "kind_live")


# 埋め込みの見た目(フッター/色)だけをプラットフォームで切り替える。YouTubeは従来値のまま。
EMBED_STYLES = {
    "youtube": {"footer": "YouTube Notification Bot", "color": 16711680},
    "twitch": {"footer": "Twitch Notification Bot", "color": 0x9146FF},
}


def build_stream_payload(data, classification, custom_message=None, platform="youtube", language="ja"):
    """配信通知のDiscordペイロード(本文＋埋め込み)を組み立てる。"""
    style = EMBED_STYLES.get(platform, EMBED_STYLES["youtube"])
    if custom_message:
        content_text = custom_message.replace("{name}", data['author_name'])\
                                     .replace("{title}", data['title'])\
                                     .replace("{link}", data['link'])
        if "@everyone" not in content_text and "@here" not in content_text:
            content_text += " @everyone"
    else:
        content_text = build_default_content_text(data, classification, language) + " @everyone"

    payload = {
        "content": content_text,
        "embeds": [{
            "title": data['title'],
            "url": data['link'],
            "color": style["color"],
            "image": {
                "url": data['thumbnail_url']
            },
            "author": {
                "name": data['author_name']
            },
            "footer": {
                "text": style["footer"]
            },
            "timestamp": datetime.utcnow().isoformat()
        }]
    }
    return payload


def send_discord_message(channel_id, data, classification, custom_message=None, platform="youtube",
                         language="ja"):
    """通知を送り、結果(SEND_OK / SEND_CHANNEL_MISSING / SEND_FAILED)を返す。
    custom_message が無いときは language(サーバーの言語設定)の既定文言を送る。"""
    return send_stream_notification(channel_id, data, classification, custom_message, platform, language)[0]


def send_stream_notification(channel_id, data, classification, custom_message=None, platform="youtube",
                             language="ja"):
    """送信結果と、送ったメッセージID(失敗時はNone)を返す。
    メッセージIDは、Twitchの配信終了後にリンクをアーカイブへ差し替えるために保存する。"""
    payload = build_stream_payload(data, classification, custom_message, platform, language)

    try:
        response = requests.post(
            DISCORD_API_URL.format(channel_id),
            headers=HEADERS,
            json=payload,
            timeout=10
        )
        if response.status_code in [200, 204]:
            return SEND_OK, _message_id(response)

        # --- 責任分界点を明確にしてログを分ける ---
        body_preview = response.text[:200]
        if response.status_code == 401:
            # Botトークン自体が無効。運用側(自分たち)のシステム設定エラー
            logger.error(
                f"[system_error:discord_auth] Bot認証に失敗しました(トークン無効の可能性)。"
                f" channel_id={channel_id} status={response.status_code} body={body_preview}"
            )
        elif response.status_code == 403:
            # Botに権限がない＝通知先サーバー側の設定ミス
            logger.error(
                f"[client_config_error:discord_permission] チャンネルへの書き込み権限がありません"
                f"（Botの権限不足、またはロール設定・チャンネル権限の問題の可能性）。"
                f" channel_id={channel_id} status={response.status_code} body={body_preview}"
            )
        elif response.status_code == 404:
            # チャンネルが存在しない＝削除された等、通知先サーバー側の設定ミス。
            # 呼び出し元で連続回数を数え、規定回数に達したら通知設定を自動削除する。
            logger.error(
                f"[client_config_error:discord_channel_missing] 送信先チャンネルが見つかりません"
                f"（チャンネル削除、またはID誤りの可能性）。"
                f" channel_id={channel_id} status={response.status_code} body={body_preview}"
            )
            return SEND_CHANNEL_MISSING, None
        elif 500 <= response.status_code < 600:
            # Discord側の障害。こちらの設定ミスでもシステム内部エラーでもない
            logger.error(
                f"[external_error:discord_outage] Discord側のシステムエラーです(一時的な可能性)。"
                f" channel_id={channel_id} status={response.status_code} body={body_preview}"
            )
        else:
            # その他の4xxなど。基本的にはリクエスト/設定側の問題として扱う
            logger.error(
                f"[client_config_error:discord_other] Discord APIがエラーを返しました。"
                f" channel_id={channel_id} status={response.status_code} body={body_preview}"
            )
            # ステータスが404以外でも、エラーコードがUnknown Channelなら宛先不明として扱う
            if _is_unknown_channel(response):
                return SEND_CHANNEL_MISSING, None
        return SEND_FAILED, None
    except requests.exceptions.RequestException as e:
        # 接続不能・タイムアウトなど＝ネットワーク/システム側の問題
        logger.error(
            f"[system_error:network] Discordへのリクエストに失敗しました"
            f"（ネットワーク接続の問題の可能性）。 channel_id={channel_id} error={e}"
        )
        return SEND_FAILED, None


def edit_message_embed(channel_id, message_id, embed):
    """送信済みメッセージの埋め込みだけを差し替える(本文とメンションはそのまま)。
    Twitchの配信終了後に、リンクをアーカイブ(VOD)へ向け直すために使う。"""
    try:
        response = requests.patch(
            DISCORD_MESSAGE_URL.format(channel_id, message_id),
            headers=HEADERS,
            json={"embeds": [embed]},
            timeout=10,
        )
        if response.status_code in [200, 204]:
            return SEND_OK
        if response.status_code == 404 or _is_unknown_channel(response):
            # 通知が削除された、またはチャンネルが消えた。差し替えようがないので諦める。
            logger.info(
                f"[twitch_archive] 差し替え対象のメッセージが見つかりません。"
                f" channel_id={channel_id} message_id={message_id} status={response.status_code}"
            )
            return SEND_CHANNEL_MISSING
        logger.warning(
            f"[twitch_archive] アーカイブリンクへの差し替えに失敗しました。"
            f" channel_id={channel_id} message_id={message_id} status={response.status_code}"
            f" body={response.text[:200]}"
        )
        return SEND_FAILED
    except requests.exceptions.RequestException as e:
        logger.warning(
            f"[twitch_archive] 差し替えのリクエストに失敗しました（ネットワーク）。"
            f" channel_id={channel_id} message_id={message_id} error={e}"
        )
        return SEND_FAILED


def _message_id(response):
    try:
        return int(response.json()["id"])
    except (ValueError, KeyError, TypeError):
        return None


def send_followup_members_only_message(channel_id, data, language="ja"):
    """5分後の再確認でメンバー限定と判明した場合に送る追記メッセージ。"""
    payload = {
        "content": t(language, "members_only_followup", name=data['author_name'], link=data['link'])
    }
    try:
        response = requests.post(
            DISCORD_API_URL.format(channel_id),
            headers=HEADERS,
            json=payload,
            timeout=10,
        )
        if response.status_code in [200, 204]:
            return True
        logger.error(
            f"[client_config_error:discord_other] メンバー限定追記メッセージの送信に失敗しました。"
            f" channel_id={channel_id} status={response.status_code} body={response.text[:200]}"
        )
        return False
    except requests.exceptions.RequestException as e:
        logger.error(
            f"[system_error:network] メンバー限定追記メッセージ送信でネットワークエラー。"
            f" channel_id={channel_id} error={e}"
        )
        return False


def _is_unknown_channel(response) -> bool:
    """Discordの応答が「チャンネルが存在しない(code 10003)」かどうか。"""
    try:
        return response.json().get("code") == DISCORD_UNKNOWN_CHANNEL
    except ValueError:
        return False
