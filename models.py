from sqlalchemy import (Column, Integer, String, Boolean, DateTime, BigInteger, CheckConstraint,
                        ForeignKeyConstraint, UniqueConstraint, create_engine)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from sqlalchemy.sql import func
from config import Config

engine = create_engine(Config.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# 通知元プラットフォーム。既存レコードはすべて youtube (migrations/2026-09-22_add_platform_twitch.sql)。
PLATFORM_YOUTUBE = "youtube"
PLATFORM_TWITCH = "twitch"
PLATFORMS = (PLATFORM_YOUTUBE, PLATFORM_TWITCH)
_PLATFORM_CHECK = "platform IN ('youtube', 'twitch')"

# --- テーブル定義 ---

class Broadcaster(Base):
    __tablename__ = 'broadcasters'
    # 同じ文字列IDでもプラットフォームが違えば別チャンネルとして登録できる。
    __table_args__ = (
        UniqueConstraint('platform', 'channel_id', name='uq_broadcasters_platform_channel_id'),
        CheckConstraint(_PLATFORM_CHECK, name='ck_broadcasters_platform'),
    )

    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String, nullable=False, default=PLATFORM_YOUTUBE, server_default=PLATFORM_YOUTUBE)
    # プラットフォーム内で不変のID。YouTube: UCから始まるチャンネルID / Twitch: broadcaster_user_id
    channel_id = Column(String, index=True, nullable=False)
    name = Column(String)
    is_active = Column(Boolean, default=True)
    last_subscribed_at = Column(DateTime, nullable=True)
    # Last hub-confirmed lease (UTC); POST acceptance never updates this expiry.
    subscription_expires_at = Column(DateTime(timezone=True), nullable=True)
    subscription_state = Column(String, nullable=False, default="unknown", server_default="unknown")
    subscription_checked_at = Column(DateTime(timezone=True), nullable=True)
    subscription_last_error = Column(String, nullable=True)
    subscription_failure_count = Column(Integer, nullable=False, default=0, server_default="0")
    # Independent renewal state preserves a valid lease while verification/retry is pending.
    subscription_renewal_state = Column(String, nullable=False, default="idle", server_default="idle")
    subscription_pending_since = Column(DateTime(timezone=True), nullable=True)
    subscription_renewal_baseline_at = Column(DateTime(timezone=True), nullable=True)
    subscription_renewed_at = Column(DateTime(timezone=True), nullable=True)
    subscription_next_check_at = Column(DateTime(timezone=True), nullable=True)
    subscription_next_action = Column(String, nullable=True)
    subscription_retry_count = Column(Integer, nullable=False, default=0, server_default="0")
    subscription_verify_count = Column(Integer, nullable=False, default=0, server_default="0")
    subscription_last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    subscription_cycle_started_at = Column(DateTime(timezone=True), nullable=True)
    # 4xx/denied require configuration repair and explicit unblock; no blind retries.
    subscription_blocked = Column(Boolean, nullable=False, default=False, server_default="false")
    # --- Twitch専用(YouTube行ではすべてNULL)。上の subscription_* はYouTube(WebSub)用。 ---
    # ログイン名は配信者が変更できるため、通知受信時に最新値へ追従させる。
    twitch_login = Column(String, nullable=True)
    twitch_broadcaster_user_id = Column(String, nullable=True)
    twitch_subscription_id = Column(String, nullable=True)
    # Twitchの購読status(enabled / webhook_callback_verification_pending /
    # notification_failures_exceeded 等)、または作成失敗時の 'error'。
    twitch_subscription_status = Column(String, nullable=True)
    twitch_subscription_checked_at = Column(DateTime(timezone=True), nullable=True)
    twitch_subscription_error = Column(String, nullable=True)
    # 配信終了(stream.offline)の購読。終了後に通知のリンクをアーカイブへ差し替えるために使う。
    twitch_offline_subscription_id = Column(String, nullable=True)
    twitch_offline_subscription_status = Column(String, nullable=True)
    twitch_offline_subscription_error = Column(String, nullable=True)
    subscriptions = relationship("Subscription", back_populates="broadcaster")

class Subscription(Base):
    __tablename__ = 'subscriptions'
    __table_args__ = (
        ForeignKeyConstraint(['platform', 'youtube_channel_id'],
                             ['broadcasters.platform', 'broadcasters.channel_id'],
                             name='fk_subscriptions_broadcaster'),
        CheckConstraint(_PLATFORM_CHECK, name='ck_subscriptions_platform'),
    )

    id = Column(Integer, primary_key=True, index=True)
    platform = Column(String, nullable=False, default=PLATFORM_YOUTUBE, server_default=PLATFORM_YOUTUBE)
    
    # ★追加: どのサーバー(Guild)の設定かを区別する
    discord_guild_id = Column(BigInteger, nullable=False)
    
    discord_channel_id = Column(BigInteger, nullable=False)
    # 歴史的経緯で列名はyoutube_のままだが、実体は broadcasters.channel_id (platformとの複合FK)。
    # Twitch行では broadcaster_user_id が入る。旧コードとの互換のため列名は変えない。
    youtube_channel_id = Column(String, nullable=False)
    custom_message = Column(String, nullable=True)

    # ★追加: 通知先チャンネルが見つからない(404 Unknown Channel)エラーが「連続した」回数。
    #   送信に成功した時点で0に戻すため、単発の失敗では増え続けない。
    #   規定回数に達した通知設定は tasks.py 側で自動削除される。
    #   ※既存DBには models.py の変更だけでは列が増えないため、
    #     migrations/2026-08-17_add_subscription_fail_count.sql の実行が必要。
    fail_count = Column(Integer, nullable=False, default=0, server_default="0")

    created_at = Column(DateTime, default=func.now())

    broadcaster = relationship("Broadcaster", back_populates="subscriptions")

class LiveStream(Base):
    __tablename__ = 'live_streams'

    id = Column(Integer, primary_key=True, index=True)
    video_id = Column(String, unique=True, index=True, nullable=False)
    title = Column(String)
    published_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())

class TwitchEventSubMessage(Base):
    """受信済みEventSubメッセージID。Twitchの再送(同じMessage-Id)を二重処理しないために使う。
    10分より古いメッセージは署名が正しくても受け付けないため、1日で削除してよい。"""
    __tablename__ = 'twitch_eventsub_messages'

    message_id = Column(String, primary_key=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)

class TwitchStreamNotification(Base):
    """Twitchの配信開始通知として送ったDiscordメッセージ。配信終了後にこの行をたどり、
    埋め込みのリンクをアーカイブ(VOD)へ差し替える。差し替え済み・断念した行は archived=True。"""
    __tablename__ = 'twitch_stream_notifications'

    id = Column(Integer, primary_key=True, index=True)
    stream_id = Column(String, nullable=False, index=True)
    broadcaster_user_id = Column(String, nullable=False, index=True)
    discord_channel_id = Column(BigInteger, nullable=False)
    discord_message_id = Column(BigInteger, nullable=False)
    archived = Column(Boolean, nullable=False, default=False, server_default="false")
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now(), server_default=func.now())

class NotificationLog(Base):
    __tablename__ = 'notification_logs'
    id = Column(Integer, primary_key=True, index=True)
    message = Column(String)
    created_at = Column(DateTime, default=func.now())

class AppSetting(Base):
    """管理画面から変更する実行時設定(例: ライブポーリングの有効/無効)を保持する。
    コンテナ再起動なしで反映するため、Celeryタスク側が毎回このテーブルを参照する。"""
    __tablename__ = 'app_settings'

    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)

class GuildSetting(Base):
    """サーバー(Guild)ごとの設定。/language で選んだ表示言語(ja/en/ko/zh)を保持する。
    行が無いサーバーは ja 扱い(i18n.py)。新規テーブルなので init_db() の create_all で作られる。"""
    __tablename__ = 'guild_settings'

    discord_guild_id = Column(BigInteger, primary_key=True, autoincrement=False)
    language = Column(String, nullable=False, default="ja", server_default="ja")
    updated_at = Column(DateTime(timezone=True), nullable=False, default=func.now(),
                        server_default=func.now(), onupdate=func.now())

def init_db():
    Base.metadata.create_all(bind=engine)
