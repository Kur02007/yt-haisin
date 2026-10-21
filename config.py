import os
import re
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv()

class Config:
    DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL")
    YOUTUBE_WEBHOOK_SECRET = os.getenv("YOUTUBE_WEBHOOK_SECRET")
    # videos.list(1動画IDにつきクオータ1)でプレミア/メンバー限定の判定を補強するために使用
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

    # 通知を受信する web(FastAPI) の公開コールバックURL。
    # bot.py / tasks.py で重複定義していたものを一元化（env で上書き可能）。
    CALLBACK_URL = os.getenv(
        "CALLBACK_URL", "http://163.44.110.157:8195/webhook/youtube"
    )

    # 毎時何分に全購読を照会するか。正常な実期限に余裕があればPOSTしない。
    SUBSCRIPTION_RENEWAL_SCHEDULE_MINUTE = int(os.getenv("SUBSCRIPTION_RENEWAL_SCHEDULE_MINUTE", "7"))
    # ハブが確認した残り期限がこの日数以下なら更新を始める。
    SUBSCRIPTION_RENEW_BEFORE_DAYS = float(os.getenv("SUBSCRIPTION_RENEW_BEFORE_DAYS", "5"))
    # 警告・緊急はPOSTの成否でなく実期限に基づく。
    SUBSCRIPTION_WARNING_HOURS = float(os.getenv("SUBSCRIPTION_WARNING_HOURS", "24"))
    SUBSCRIPTION_CRITICAL_HOURS = float(os.getenv("SUBSCRIPTION_CRITICAL_HOURS", "6"))
    # 緊急対象だけ追加照会する間隔(分)。15〜30分に制限する。
    SUBSCRIPTION_URGENT_CHECK_MINUTES = int(os.getenv("SUBSCRIPTION_URGENT_CHECK_MINUTES", "20"))
    # 初回POSTとは別の最大再試行回数(0〜3)。30/90/300秒に±20%の揺らぎ。
    SUBSCRIPTION_RETRY_ATTEMPTS = int(os.getenv("SUBSCRIPTION_RETRY_ATTEMPTS", "3"))
    SUBSCRIPTION_RETRY_BASE_SECONDS = float(os.getenv("SUBSCRIPTION_RETRY_BASE_SECONDS", "30"))
    # GET/POSTそれぞれのネットワークタイムアウト(秒)。
    SUBSCRIPTION_REQUEST_TIMEOUT_SECONDS = float(os.getenv("SUBSCRIPTION_REQUEST_TIMEOUT_SECONDS", "45"))
    if not (0 <= SUBSCRIPTION_RENEWAL_SCHEDULE_MINUTE <= 59
            and 0 <= SUBSCRIPTION_RETRY_ATTEMPTS <= 3
            and 15 <= SUBSCRIPTION_URGENT_CHECK_MINUTES <= 30
            and 0 < SUBSCRIPTION_CRITICAL_HOURS <= SUBSCRIPTION_WARNING_HOURS
            and SUBSCRIPTION_RENEW_BEFORE_DAYS > 0
            and SUBSCRIPTION_RETRY_BASE_SECONDS >= 1
            and SUBSCRIPTION_REQUEST_TIMEOUT_SECONDS > 0):
        raise ValueError("Invalid subscription timing configuration")

    # PubSubHubbubはライブ配信の「開始」検知が遅い(数分〜、場合によってはそれ以上)ため、
    # 補助的にチャンネルのライブ状態を短い間隔でポーリングして即時性を確保する。
    ENABLE_LIVE_POLLING = os.getenv("ENABLE_LIVE_POLLING", "true").lower() == "true"
    LIVE_POLL_INTERVAL_SECONDS = int(os.getenv("LIVE_POLL_INTERVAL_SECONDS", "45"))
    LIVE_POLL_TIMEOUT_SECONDS = int(os.getenv("LIVE_POLL_TIMEOUT_SECONDS", "10"))

    # 1つのDiscordサーバー(Guild)が登録できるチャンネル数の上限(YouTube・Twitch合計)。
    # 通知先(subscriptions)の行数で数える。既存登録の通知先変更・メッセージ変更は上限の対象外。
    MAX_CHANNELS_PER_GUILD = int(os.getenv("MAX_CHANNELS_PER_GUILD", "20"))
    if not 1 <= MAX_CHANNELS_PER_GUILD <= 1000:
        raise ValueError("Invalid MAX_CHANNELS_PER_GUILD")

    # 管理ダッシュボード(/admin/api/*)の認証キー。未設定の場合は管理APIを全拒否する。
    ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")

    # --- Twitch EventSub (任意機能。4つすべて揃ったときだけ有効になる) ---
    # 値はログ・例外メッセージに出さないこと(twitch_api.py は固定のエラーコードのみ記録する)。
    TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
    TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
    # EventSub の署名検証用シークレット。Twitch仕様で10〜100文字のASCII。
    TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")
    # Twitchから到達できる公開HTTPS URL(ポート443必須)。例: https://example.com/webhook/twitch
    TWITCH_CALLBACK_URL = os.getenv("TWITCH_CALLBACK_URL")
    # 毎時何分にTwitch購読の存在確認(失効時の再購読)を行うか。YouTube側の:07とずらす。
    TWITCH_RECONCILE_SCHEDULE_MINUTE = int(os.getenv("TWITCH_RECONCILE_SCHEDULE_MINUTE", "37"))
    TWITCH_REQUEST_TIMEOUT_SECONDS = float(os.getenv("TWITCH_REQUEST_TIMEOUT_SECONDS", "10"))
    if not 0 <= TWITCH_RECONCILE_SCHEDULE_MINUTE <= 59:
        raise ValueError("Invalid TWITCH_RECONCILE_SCHEDULE_MINUTE")

    @classmethod
    def twitch_config_error(cls):
        """Twitch設定の不備を固定コードで返す(問題なければNone)。値そのものは返さない。"""
        if not (cls.TWITCH_CLIENT_ID and cls.TWITCH_CLIENT_SECRET
                and cls.TWITCH_WEBHOOK_SECRET and cls.TWITCH_CALLBACK_URL):
            return "twitch_not_configured"
        secret = cls.TWITCH_WEBHOOK_SECRET
        if not (10 <= len(secret) <= 100 and secret.isascii()):
            return "twitch_webhook_secret_invalid"
        url = cls.TWITCH_CALLBACK_URL
        if not url.startswith("https://") or re.match(r"https://[^/]+:(?!443(?:/|$))\d+", url):
            return "twitch_callback_url_invalid"
        return None

    @classmethod
    def twitch_enabled(cls):
        return cls.twitch_config_error() is None

    # 設定が読み込めていない場合にエラーを出す（ミス防止）
    if not DISCORD_BOT_TOKEN:
        raise ValueError("DISCORD_BOT_TOKEN が .env に設定されていません")
