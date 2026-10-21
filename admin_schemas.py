from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

from channel_registration import parse_youtube_channel_id, parse_youtube_handle
from twitch_eventsub import parse_twitch_login

Platform = Literal["youtube", "twitch"]


class ChannelOut(BaseModel):
    platform: Platform = "youtube"
    channel_id: str
    name: Optional[str] = None
    is_active: bool
    subscriber_count: int
    last_subscribed_at: Optional[datetime] = None
    url: Optional[str] = None
    twitch_login: Optional[str] = None
    # YouTube: WebSub購読の状態(subscription_state) / Twitch: EventSub購読のstatus
    subscription_status: Optional[str] = None
    subscription_error: Optional[str] = None
    offline_subscription_status: Optional[str] = None


class ChannelUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    name: Optional[str] = Field(default=None, max_length=100)


class ChannelCreateRequest(BaseModel):
    platform: Platform
    # YouTube: チャンネルID(UC...)・チャンネルURL・ハンドルURL(@name) / Twitch: チャンネル名またはURL
    identifier: str = Field(min_length=1, max_length=200)
    # Discordのsnowflakeは2^53を超えるため、JSONでは文字列で受け取る
    discord_guild_id: Optional[str] = Field(default=None, pattern=r"^\d{15,20}$")
    discord_channel_id: Optional[str] = Field(default=None, pattern=r"^\d{15,20}$")
    custom_message: Optional[str] = Field(default=None, max_length=1000)

    @model_validator(mode="after")
    def check_by_platform(self):
        self.identifier = self.identifier.strip()
        if self.platform == "youtube" and not (parse_youtube_channel_id(self.identifier)
                                               or parse_youtube_handle(self.identifier)):
            raise ValueError("YouTubeのチャンネルID（UCから始まる24文字）を含むURL、"
                             "またはハンドルURL（例: https://www.youtube.com/@name）を指定してください")
        if self.platform == "twitch" and not parse_twitch_login(self.identifier):
            raise ValueError("Twitchのチャンネル名またはURL（例: https://www.twitch.tv/名前）を指定してください")
        if (self.discord_guild_id is None) != (self.discord_channel_id is None):
            raise ValueError("通知先を指定する場合はサーバーIDとチャンネルIDの両方を入力してください")
        if self.custom_message is not None and not self.custom_message.strip():
            self.custom_message = None
        return self


class ScheduleStatus(BaseModel):
    live_polling_enabled: bool
    live_poll_interval_seconds: int


class ScheduleUpdateRequest(BaseModel):
    live_polling_enabled: bool
