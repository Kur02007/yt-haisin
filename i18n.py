"""サーバー(Guild)ごとの表示言語。/language で選び、guild_settings テーブルに保存する。

ja の文言は多言語化前の文言と同一にしてある(未設定サーバーの見た目は変わらない)。
翻訳が見つからないキーは ja にフォールバックする。
"""
import logging

from sqlalchemy.exc import SQLAlchemyError

from models import GuildSetting, SessionLocal

logger = logging.getLogger(__name__)

DEFAULT_LANGUAGE = "ja"

# /language の選択肢に出す表示名
LANGUAGE_NAMES = {
    "ja": "日本語",
    "en": "English",
    "ko": "한국어",
    "zh": "中文",
}
LANGUAGES = tuple(LANGUAGE_NAMES)

MESSAGES = {
    # --- /language ---
    "language_set": {
        "ja": "🌐 このサーバーの言語を **日本語** に設定しました。",
        "en": "🌐 This server's language has been set to **English**.",
        "ko": "🌐 이 서버의 언어를 **한국어**로 설정했습니다.",
        "zh": "🌐 已将本服务器的语言设置为 **中文**。",
    },
    # --- /add_youtube ---
    "this_channel": {
        "ja": "このチャンネル",
        "en": "this channel",
        "ko": "이 채널",
        "zh": "此频道",
    },
    "youtube_created": {
        "ja": "✅ **新規登録しました！**\nID: `{youtube_id}`\n通知先: {channel}\nメッセージ: `{message}`",
        "en": "✅ **Registered!**\nID: `{youtube_id}`\nNotify in: {channel}\nMessage: `{message}`",
        "ko": "✅ **새로 등록했습니다!**\nID: `{youtube_id}`\n알림 채널: {channel}\n메시지: `{message}`",
        "zh": "✅ **已新增登记！**\nID: `{youtube_id}`\n通知频道: {channel}\n消息: `{message}`",
    },
    "default_message": {
        "ja": "デフォルト設定",
        "en": "Default",
        "ko": "기본 설정",
        "zh": "默认设置",
    },
    "youtube_updated": {
        "ja": "🔄 **設定を更新しました！** (サーバー: {guild})\n通知先: {channel}\n{message_status}",
        "en": "🔄 **Settings updated!** (Server: {guild})\nNotify in: {channel}\n{message_status}",
        "ko": "🔄 **설정을 업데이트했습니다!** (서버: {guild})\n알림 채널: {channel}\n{message_status}",
        "zh": "🔄 **设置已更新！** (服务器: {guild})\n通知频道: {channel}\n{message_status}",
    },
    "message_set": {
        "ja": "設定: `{message}`",
        "en": "Message: `{message}`",
        "ko": "메시지: `{message}`",
        "zh": "消息: `{message}`",
    },
    "message_unchanged": {
        "ja": "メッセージ変更なし",
        "en": "Message unchanged",
        "ko": "메시지 변경 없음",
        "zh": "消息未更改",
    },
    "system_error": {
        "ja": "❌ システム内部エラーが発生しました。時間をおいて再度お試しください。",
        "en": "❌ An internal error occurred. Please try again later.",
        "ko": "❌ 시스템 내부 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        "zh": "❌ 发生系统内部错误，请稍后再试。",
    },
    "unknown_error": {
        "ja": "❌ エラー: {error}",
        "en": "❌ Error: {error}",
        "ko": "❌ 오류: {error}",
        "zh": "❌ 错误: {error}",
    },
    # RegistrationError.code ごとの説明(ja は channel_registration 側の文言をそのまま使う)
    "error_invalid_input": {
        "en": "❌ Please specify a YouTube channel ID (24 characters starting with UC), a channel URL, "
              "or a handle URL (e.g. https://www.youtube.com/@name).",
        "ko": "❌ YouTube 채널 ID(UC로 시작하는 24자), 채널 URL 또는 핸들 URL"
              "(예: https://www.youtube.com/@name)을 입력해 주세요.",
        "zh": "❌ 请输入 YouTube 频道 ID（以 UC 开头的 24 个字符）、频道 URL "
              "或账号 URL（例: https://www.youtube.com/@name）。",
    },
    "error_not_found": {
        "en": "❌ YouTube channel \"@{handle}\" was not found. Please check the handle.",
        "ko": "❌ YouTube 채널 \"@{handle}\"을(를) 찾을 수 없습니다. 핸들을 확인해 주세요.",
        "zh": "❌ 找不到 YouTube 频道「@{handle}」。请检查账号名称。",
    },
    "error_not_configured": {
        "en": "❌ Cannot look up handle URLs (YOUTUBE_API_KEY is not set). "
              "Use a URL containing the channel ID (UC...) or contact the administrator.",
        "ko": "❌ 핸들 URL로 채널 ID를 조회할 수 없습니다(YOUTUBE_API_KEY 미설정). "
              "채널 ID(UC...)가 포함된 URL을 입력하거나 관리자에게 문의해 주세요.",
        "zh": "❌ 无法通过账号 URL 查询频道 ID（未设置 YOUTUBE_API_KEY）。"
              "请使用包含频道 ID（UC...）的 URL，或联系管理员。",
    },
    "error_quota_exceeded": {
        "en": "❌ The YouTube API usage limit has been reached. Please try again later.",
        "ko": "❌ YouTube API 사용 한도에 도달했습니다. 잠시 후 다시 시도해 주세요.",
        "zh": "❌ 已达到 YouTube API 使用上限，请稍后再试。",
    },
    "error_guild_limit_reached": {
        "en": "❌ This server has reached the limit of {max} channels (YouTube + Twitch). "
              "Remove unused ones with /remove_youtube or /remove_twitch and try again.",
        "ko": "❌ 이 서버의 등록 한도({max}개, YouTube·Twitch 합계)에 도달했습니다. "
              "/remove_youtube 또는 /remove_twitch로 불필요한 등록을 삭제한 뒤 다시 시도해 주세요.",
        "zh": "❌ 本服务器已达到登记上限（{max} 个，YouTube 与 Twitch 合计）。"
              "请先用 /remove_youtube 或 /remove_twitch 删除不需要的登记后再试。",
    },
    "error_lookup_failed": {
        "en": "❌ Failed to verify the YouTube channel ({code}). Please try again later.",
        "ko": "❌ YouTube 채널 확인에 실패했습니다({code}). 잠시 후 다시 시도해 주세요.",
        "zh": "❌ YouTube 频道验证失败（{code}），请稍后再试。",
    },
    # --- /add_twitch ---
    "twitch_no_permission": {
        "ja": "❌ Botに{channel}への投稿権限がありません。「チャンネルを見る」「メッセージを送信」「埋め込みリンク」を許可してください。",
        "en": "❌ The bot cannot post in {channel}. Please allow \"View Channel\", \"Send Messages\" and \"Embed Links\".",
        "ko": "❌ 봇에게 {channel}에 글을 쓸 권한이 없습니다. \"채널 보기\", \"메시지 보내기\", \"링크 첨부\" 권한을 허용해 주세요.",
        "zh": "❌ 机器人没有在 {channel} 发言的权限。请允许“查看频道”“发送消息”“嵌入链接”。",
    },
    "unexpected_error": {
        "ja": "❌ 予期しないエラーが発生しました。時間をおいて再度お試しください。",
        "en": "❌ An unexpected error occurred. Please try again later.",
        "ko": "❌ 예기치 않은 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.",
        "zh": "❌ 发生意外错误，请稍后再试。",
    },
    "twitch_unchanged": {
        "ja": "ℹ️ **このサーバーでは既に登録済みです**（通知先・メッセージに変更なし）\n{twitch}\n通知先: {channel}{subscription}",
        "en": "ℹ️ **Already registered in this server** (channel and message unchanged)\n{twitch}\nNotify in: {channel}{subscription}",
        "ko": "ℹ️ **이 서버에 이미 등록되어 있습니다** (알림 채널·메시지 변경 없음)\n{twitch}\n알림 채널: {channel}{subscription}",
        "zh": "ℹ️ **本服务器已登记过此频道**（通知频道与消息均未更改）\n{twitch}\n通知频道: {channel}{subscription}",
    },
    "twitch_updated": {
        "ja": "🔄 **設定を更新しました！** (サーバー: {guild})\n{twitch}\n通知先: {channel}\n{message_status}{subscription}",
        "en": "🔄 **Settings updated!** (Server: {guild})\n{twitch}\nNotify in: {channel}\n{message_status}{subscription}",
        "ko": "🔄 **설정을 업데이트했습니다!** (서버: {guild})\n{twitch}\n알림 채널: {channel}\n{message_status}{subscription}",
        "zh": "🔄 **设置已更新！** (服务器: {guild})\n{twitch}\n通知频道: {channel}\n{message_status}{subscription}",
    },
    "twitch_created": {
        "ja": "✅ **Twitchチャンネルを新規登録しました！**\n{twitch}\n通知先: {channel}\nメッセージ: `{message}`{subscription}",
        "en": "✅ **Twitch channel registered!**\n{twitch}\nNotify in: {channel}\nMessage: `{message}`{subscription}",
        "ko": "✅ **Twitch 채널을 새로 등록했습니다!**\n{twitch}\n알림 채널: {channel}\n메시지: `{message}`{subscription}",
        "zh": "✅ **已新增登记 Twitch 频道！**\n{twitch}\n通知频道: {channel}\n消息: `{message}`{subscription}",
    },
    "eventsub_enabled": {
        "ja": "📡 EventSub購読: 有効",
        "en": "📡 EventSub subscription: enabled",
        "ko": "📡 EventSub 구독: 활성",
        "zh": "📡 EventSub 订阅: 已启用",
    },
    # 先頭の ⚠️ は bot.py が「成功メッセージを自動削除しない」判定に使う。どの言語でも残すこと。
    "eventsub_failed": {
        "ja": "⚠️ 登録は保存しましたが、EventSub購読の作成に失敗しました。\n原因: {reason}\n毎時の自動確認で再試行します。",
        "en": "⚠️ The registration was saved, but creating the EventSub subscription failed.\nReason: {reason}\nThe hourly check will retry automatically.",
        "ko": "⚠️ 등록은 저장되었지만 EventSub 구독 생성에 실패했습니다.\n원인: {reason}\n매시간 자동 확인에서 다시 시도합니다.",
        "zh": "⚠️ 登记已保存，但创建 EventSub 订阅失败。\n原因: {reason}\n每小时的自动检查会重试。",
    },
    # --- /remove_youtube・/remove_twitch ---
    "removed": {
        "ja": "🗑️ **通知登録を削除しました**\n{label}\nこのサーバーの登録: {remaining}/{max}\n※ 再登録は /add_youtube・/add_twitch でいつでもできます。",
        "en": "🗑️ **Notification removed**\n{label}\nRegistrations in this server: {remaining}/{max}\n※ You can register again anytime with /add_youtube or /add_twitch.",
        "ko": "🗑️ **알림 등록을 삭제했습니다**\n{label}\n이 서버의 등록: {remaining}/{max}\n※ /add_youtube·/add_twitch로 언제든지 다시 등록할 수 있습니다.",
        "zh": "🗑️ **已删除通知登记**\n{label}\n本服务器的登记: {remaining}/{max}\n※ 随时可以用 /add_youtube 或 /add_twitch 重新登记。",
    },
    # --- エラー(ja は channel_registration 側の文言をそのまま使うので訳だけ置く) ---
    "error_twitch_invalid_input": {
        "en": "❌ Please specify a Twitch channel name or URL (e.g. https://www.twitch.tv/name).",
        "ko": "❌ Twitch 채널 이름 또는 URL(예: https://www.twitch.tv/이름)을 입력해 주세요.",
        "zh": "❌ 请输入 Twitch 频道名称或 URL（例: https://www.twitch.tv/名称）。",
    },
    "error_twitch_not_found": {
        "en": "❌ Twitch channel \"{login}\" was not found. Please check the channel name.",
        "ko": "❌ Twitch 채널 \"{login}\"을(를) 찾을 수 없습니다. 채널 이름을 확인해 주세요.",
        "zh": "❌ 找不到 Twitch 频道「{login}」。请检查频道名称。",
    },
    "error_twitch_api": {
        "en": "❌ Failed to verify the Twitch channel ({code}). Please try again later.",
        "ko": "❌ Twitch 채널 확인에 실패했습니다({code}). 잠시 후 다시 시도해 주세요.",
        "zh": "❌ Twitch 频道验证失败（{code}），请稍后再试。",
    },
    "error_twitch_not_configured": {
        "en": "❌ The Twitch integration is not configured correctly ({code}). Please contact the administrator.",
        "ko": "❌ Twitch 연동 설정이 올바르지 않습니다({code}). 관리자에게 문의해 주세요.",
        "zh": "❌ Twitch 集成未正确配置（{code}），请联系管理员。",
    },
    "error_remove_no_match_youtube": {
        "en": "❌ No registration matches \"{identifier}\". Specify the channel name or a URL containing the "
              "channel ID (24 characters starting with UC). Picking from the suggestions is the most reliable.",
        "ko": "❌ \"{identifier}\"와(과) 일치하는 등록이 없습니다. 채널 이름 또는 채널 ID(UC로 시작하는 24자)가 "
              "포함된 URL을 입력해 주세요(후보에서 선택하면 확실합니다).",
        "zh": "❌ 找不到与「{identifier}」匹配的登记。请输入频道名称，或包含频道 ID（以 UC 开头的 24 个字符）的 URL"
              "（从候选中选择最可靠）。",
    },
    "error_remove_no_match_twitch": {
        "en": "❌ No registration matches \"{identifier}\". Specify the channel name, login name, or URL. "
              "Picking from the suggestions is the most reliable.",
        "ko": "❌ \"{identifier}\"와(과) 일치하는 등록이 없습니다. 채널 이름·로그인 이름·URL 중 하나를 입력해 주세요"
              "(후보에서 선택하면 확실합니다).",
        "zh": "❌ 找不到与「{identifier}」匹配的登记。请输入频道名称、登录名或 URL（从候选中选择最可靠）。",
    },
    "error_not_registered": {
        "en": "❌ This channel is not registered in this server ({identifier}).",
        "ko": "❌ 이 서버에 등록되지 않은 채널입니다({identifier}).",
        "zh": "❌ 本服务器未登记此频道（{identifier}）。",
    },
    "error_ambiguous_name": {
        "en": "❌ Multiple registrations match \"{identifier}\". Please pick one from the suggestions.",
        "ko": "❌ \"{identifier}\"와(과) 일치하는 등록이 여러 개 있습니다. 후보에서 다시 선택해 주세요.",
        "zh": "❌ 有多个登记与「{identifier}」匹配。请从候选中重新选择。",
    },
    "error_guild_only": {
        "en": "❌ Please run this command in a server.",
        "ko": "❌ 이 명령어는 서버 안에서 실행해 주세요.",
        "zh": "❌ 请在服务器内使用此命令。",
    },
    # --- メンバー限定の追記(配信開始の約5分後) ---
    "members_only_followup": {
        "ja": "🔒 追記：さきほどの **{name}** の配信は**メンバー限定**の可能性があります(自動判定・非公式シグナルによる推測です)。\n{link}",
        "en": "🔒 Update: **{name}**'s stream above may be **members-only** (automatic guess based on an unofficial signal).\n{link}",
        "ko": "🔒 추가: 방금 전 **{name}** 님의 방송은 **멤버십 전용**일 수 있습니다(비공식 신호를 이용한 자동 추정입니다).\n{link}",
        "zh": "🔒 补充：刚才 **{name}** 的直播可能是**会员限定**（根据非官方信号的自动推测）。\n{link}",
    },
    # --- Twitch の配信終了後(アーカイブへの差し替え) ---
    "archive_title_prefix": {"ja": "【アーカイブ】", "en": "[Archive] ", "ko": "[다시보기] ", "zh": "【回放】"},
    "archive_duration": {"ja": "配信時間", "en": "Duration", "ko": "방송 시간", "zh": "直播时长"},
    "archive_ended": {"ja": "・配信終了", "en": " · Stream ended", "ko": " · 방송 종료", "zh": "・直播已结束"},
    # --- 配信通知の本文(末尾の @everyone は discord_notifier 側で付ける) ---
    # {lock} はメンバー限定のとき 🔒、{kind} は「配信」/「メンバー限定配信」などの種別
    "notify_live": {
        "ja": "🔴{lock} **{name}** が{kind}を開始しました！",
        "en": "🔴{lock} **{name}** has started a {kind}!",
        "ko": "🔴{lock} **{name}** 님이 {kind}을 시작했습니다!",
        "zh": "🔴{lock} **{name}** 开始了{kind}！",
    },
    "notify_upcoming": {
        "ja": "📅{lock} **{name}** の{kind}が **{time}** から始まる予定です！",
        "en": "📅{lock} **{name}**'s {kind} is scheduled to start at **{time}**!",
        "ko": "📅{lock} **{name}** 님의 {kind}이 **{time}**부터 시작될 예정입니다!",
        "zh": "📅{lock} **{name}** 的{kind}预定于 **{time}** 开始！",
    },
    "notify_premiere_upcoming": {
        "ja": "🎬{lock} **{name}** の{kind}が **{time}** から始まります！",
        "en": "🎬{lock} **{name}**'s {kind} starts at **{time}**!",
        "ko": "🎬{lock} **{name}** 님의 {kind}가 **{time}**부터 시작됩니다!",
        "zh": "🎬{lock} **{name}** 的{kind}将于 **{time}** 开始！",
    },
    "notify_premiere_live": {
        "ja": "🎬{lock} **{name}** の{kind}が始まりました！",
        "en": "🎬{lock} **{name}**'s {kind} has started!",
        "ko": "🎬{lock} **{name}** 님의 {kind}가 시작되었습니다!",
        "zh": "🎬{lock} **{name}** 的{kind}开始了！",
    },
    "notify_video": {
        "ja": "📹{lock} **{name}** が{kind}を投稿しました！",
        "en": "📹{lock} **{name}** has uploaded a new {kind}!",
        "ko": "📹{lock} **{name}** 님이 {kind}을 올렸습니다!",
        "zh": "📹{lock} **{name}** 发布了{kind}！",
    },
    "kind_live": {"ja": "配信", "en": "live stream", "ko": "라이브 방송", "zh": "直播"},
    "kind_live_members": {"ja": "メンバー限定配信", "en": "members-only live stream",
                          "ko": "멤버십 전용 방송", "zh": "会员限定直播"},
    "kind_video": {"ja": "動画", "en": "video", "ko": "동영상", "zh": "视频"},
    "kind_video_members": {"ja": "メンバー限定動画", "en": "members-only video",
                           "ko": "멤버십 전용 동영상", "zh": "会员限定视频"},
    "kind_premiere": {"ja": "プレミア公開", "en": "premiere", "ko": "최초 공개", "zh": "首映"},
    "kind_premiere_members": {"ja": "メンバー限定プレミア公開", "en": "members-only premiere",
                              "ko": "멤버십 전용 최초 공개", "zh": "会员限定首映"},
}


# スラッシュコマンドの説明文の訳(bot.py の CommandTranslator が使う)。
# キーは bot.py のデコレータに書いた日本語の説明文そのもの。そちらを変えたらここも合わせる。
# サーバーの /language ではなく、見る人の Discord クライアントの言語で切り替わる
# (Discord の仕様。コマンドは全サーバー共通で一度だけ登録するため)。Discordの上限は100文字。
COMMAND_TEXTS = {
    # /language
    "Botの応答・通知の言語を設定します": {
        "en": "Set the language for bot replies and notifications",
        "ko": "봇의 응답 및 알림 언어를 설정합니다",
        "zh": "设置机器人回复和通知的语言",
    },
    "言語": {"en": "Language", "ko": "언어", "zh": "语言"},
    # /add_youtube・/add_twitch
    "YouTubeチャンネルの通知を登録・更新します": {
        "en": "Register or update notifications for a YouTube channel",
        "ko": "YouTube 채널 알림을 등록하거나 업데이트합니다",
        "zh": "登记或更新 YouTube 频道的通知",
    },
    "チャンネルURL・ハンドルURL(@name)・チャンネルID": {
        "en": "Channel URL, handle URL (@name), or channel ID",
        "ko": "채널 URL, 핸들 URL(@name) 또는 채널 ID",
        "zh": "频道 URL、账号 URL（@name）或频道 ID",
    },
    "Twitchを追加：Twitchチャンネルの配信開始通知を登録・更新します": {
        "en": "Add Twitch: register or update go-live notifications for a Twitch channel",
        "ko": "Twitch 추가: Twitch 채널의 방송 시작 알림을 등록하거나 업데이트합니다",
        "zh": "添加 Twitch：登记或更新 Twitch 频道的开播通知",
    },
    "TwitchのチャンネルURLまたはチャンネル名": {
        "en": "Twitch channel URL or channel name",
        "ko": "Twitch 채널 URL 또는 채널 이름",
        "zh": "Twitch 频道 URL 或频道名称",
    },
    "通知先チャンネル": {"en": "Channel to post notifications in", "ko": "알림을 보낼 채널", "zh": "发送通知的频道"},
    "通知メッセージ": {"en": "Notification message", "ko": "알림 메시지", "zh": "通知消息"},
    # /remove_youtube・/remove_twitch
    "YouTubeチャンネルの通知登録をこのサーバーから削除します": {
        "en": "Remove a YouTube channel's notifications from this server",
        "ko": "이 서버에서 YouTube 채널 알림 등록을 삭제합니다",
        "zh": "从本服务器删除 YouTube 频道的通知登记",
    },
    "登録済みチャンネル（候補から選択、またはチャンネルID・URL）": {
        "en": "Registered channel (pick a suggestion, or enter a channel ID/URL)",
        "ko": "등록된 채널 (후보에서 선택하거나 채널 ID·URL 입력)",
        "zh": "已登记的频道（从候选中选择，或输入频道 ID/URL）",
    },
    "Twitchチャンネルの通知登録をこのサーバーから削除します": {
        "en": "Remove a Twitch channel's notifications from this server",
        "ko": "이 서버에서 Twitch 채널 알림 등록을 삭제합니다",
        "zh": "从本服务器删除 Twitch 频道的通知登记",
    },
    "登録済みチャンネル（候補から選択、またはチャンネル名・URL）": {
        "en": "Registered channel (pick a suggestion, or enter a channel name/URL)",
        "ko": "등록된 채널 (후보에서 선택하거나 채널 이름·URL 입력)",
        "zh": "已登记的频道（从候选中选择，或输入频道名称/URL）",
    },
}


def normalize_language(language):
    return language if language in LANGUAGE_NAMES else DEFAULT_LANGUAGE


def t(language, key, **kwargs):
    """key の文言を language で返す。その言語の訳が無ければ ja を使う。"""
    texts = MESSAGES[key]
    text = texts.get(normalize_language(language)) or texts[DEFAULT_LANGUAGE]
    return text.format(**kwargs) if kwargs else text


def get_guild_language(guild_id):
    """サーバーの言語設定。未設定・DM・DBエラー時は ja(通知や応答を止めないため例外は出さない)。"""
    return get_guild_languages([guild_id]).get(guild_id, DEFAULT_LANGUAGE)


def get_guild_languages(guild_ids):
    """{guild_id: language} を1クエリで返す。設定の無いサーバーは結果に含まれない。

    独立したセッションを使うので、guild_settings がまだ無い(bot の init_db 前)などで
    失敗しても呼び出し元のトランザクションは汚れない。その場合は空(=全サーバー ja)。
    """
    ids = {gid for gid in guild_ids if gid is not None}
    if not ids:
        return {}
    try:
        with SessionLocal() as db:
            rows = db.query(GuildSetting.discord_guild_id, GuildSetting.language).filter(
                GuildSetting.discord_guild_id.in_(ids)).all()
        return {gid: normalize_language(lang) for gid, lang in rows}
    except SQLAlchemyError:
        logger.warning("Guild language lookup failed count=1; falling back to ja")
        return {}


def set_guild_language(guild_id, language):
    """サーバーの言語設定を保存(なければ作成)する。"""
    language = normalize_language(language)
    with SessionLocal() as db:
        setting = db.get(GuildSetting, guild_id)
        if setting is None:
            db.add(GuildSetting(discord_guild_id=guild_id, language=language))
        else:
            setting.language = language
        db.commit()
    return language
