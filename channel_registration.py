"""チャンネル登録の共通処理(Discordの /add_twitch と管理画面から使う)。

/add_youtube の既存処理は bot.py にそのまま残し、ここでは同じ手順
(配信者行を用意 → 通知先(Subscription)を追加/更新 → 購読を開始)を両プラットフォームで行う。
同期関数なので、discord.py からは asyncio.to_thread で呼ぶ。
"""
import logging
import re
from collections import namedtuple

from sqlalchemy.exc import IntegrityError

from config import Config
from models import PLATFORM_TWITCH, PLATFORM_YOUTUBE, Broadcaster, SessionLocal, Subscription
from subscription_tasks import request_initial_subscription
import twitch_api
import twitch_eventsub
import youtube_api
from twitch_api import TwitchAPIError
from youtube_api import YouTubeResolveError, parse_youtube_handle  # noqa: F401 (再エクスポート)

logger = logging.getLogger(__name__)

# 名前がまだ分からないチャンネルの暫定名(/add_youtube 由来の既存データにも入っている)
UNKNOWN_NAME = "Unknown"


# destination: 通知先の扱い。"created" / "updated" / "unchanged" / None(通知先の指定なし)
RegistrationResult = namedtuple(
    "RegistrationResult",
    "broadcaster_pk platform channel_id name login created_channel destination "
    "subscription_status subscription_error subscription_transient")

# remaining: 削除後にこのサーバーが登録しているチャンネル数(YouTube・Twitch合計)
RemovalResult = namedtuple("RemovalResult", "platform channel_id name login remaining")


class RegistrationError(Exception):
    """利用者にそのまま見せられる理由付きの登録失敗。http_status は管理APIの応答に使う。"""
    def __init__(self, code, message, http_status=400):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(message)


_ERROR_TEXTS = {
    "twitch_not_configured": "Twitch連携が未設定です（TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET / "
                             "TWITCH_WEBHOOK_SECRET / TWITCH_CALLBACK_URL）。管理者に連絡してください。",
    "twitch_webhook_secret_invalid": "TWITCH_WEBHOOK_SECRET は10〜100文字のASCIIで設定してください。",
    "twitch_callback_url_invalid": "TWITCH_CALLBACK_URL は https:// で始まり443番ポートのURLである必要があります。",
    "eventsub_conflict_other_callback": "同じチャンネルの購読が別のコールバックURL宛てに既に存在します。"
                                        "同じClient IDを使う他のサービスを確認してください。",
    "eventsub_create_http_400": "Twitchが購読要求を拒否しました（コールバックURLまたはWebhookシークレットの設定を確認してください）。",
    "eventsub_create_http_403": "Twitchが購読を許可しませんでした（Client IDの設定を確認してください）。",
    "token_http_400": "Twitchのアクセストークンを取得できません（Client ID / Client Secret を確認してください）。",
    "token_http_401": "Twitchのアクセストークンを取得できません（Client ID / Client Secret を確認してください）。",
    "token_http_403": "Twitchのアクセストークンを取得できません（Client ID / Client Secret を確認してください）。",
}


def describe_error(code):
    """固定エラーコードを利用者向けの説明にする(シークレット等は含まない)。"""
    if not code:
        return ""
    if code in _ERROR_TEXTS:
        return _ERROR_TEXTS[code]
    if code.startswith("revoked_"):
        return "Twitch側で購読が失効しました（%s）。再購読してください。" % code[len("revoked_"):]
    if code.startswith("network_") or code.endswith(("_http_429",)) or re.search(r"_http_5\d\d$", code):
        return "Twitch APIの一時的なエラーです（%s）。時間をおいて再試行されます。" % code
    return "Twitch APIエラー（%s）" % code


def parse_youtube_channel_id(value):
    """UC...形式のチャンネルIDを取り出す(外部アクセスなし)。見つからなければNone。"""
    return youtube_api.parse_youtube_channel_id(value)


# ハンドル変換の失敗コード→利用者向けの説明。%s にはハンドル名が入る。
_YOUTUBE_RESOLVE_TEXTS = {
    "invalid_input": "YouTubeのチャンネルID（UCから始まる24文字）、チャンネルURL、"
                     "またはハンドルURL（例: https://www.youtube.com/@name）を指定してください。",
    "not_configured": "ハンドルURLからチャンネルIDを調べられません（YOUTUBE_API_KEY が未設定）。"
                      "チャンネルID（UC...）を含むURLを指定するか、管理者に連絡してください。",
    "not_found": "YouTubeチャンネル「@%s」が見つかりません。ハンドルを確認してください。",
    "quota_exceeded": "YouTube APIの利用上限に達しました。時間をおいて再度お試しください。",
}

_YOUTUBE_RESOLVE_STATUS = {"invalid_input": 422, "not_found": 404, "not_configured": 503}


def resolve_youtube_channel(identifier):
    """チャンネルID/チャンネルURL/ハンドルURLから (channel_id, name) を返す。

    ハンドルURLのときだけ YouTube Data API(channels.list) を1チャンネルにつき1クオータ呼ぶ。
    part に snippet を含めているので、同じ1クオータで表示用のチャンネル名も分かる
    (チャンネルID指定のときは名前が分からずNone。通知が届いた時点でフィードから埋まる)。
    同じハンドルの再登録は youtube_api 側のキャッシュで追加のクオータを使わない。
    失敗は利用者にそのまま見せられる RegistrationError にする。
    """
    try:
        return youtube_api.resolve_channel(identifier)
    except YouTubeResolveError as error:
        handle = parse_youtube_handle(identifier) or ""
        text = _YOUTUBE_RESOLVE_TEXTS.get(error.code)
        if text is None:
            text = ("YouTubeチャンネルの確認に一時的に失敗しました（%s）。時間をおいて再度お試しください。"
                    % error.code) if error.transient else ("YouTubeチャンネルの確認に失敗しました（%s）。" % error.code)
        message = text % handle if "%s" in text else text
        status = _YOUTUBE_RESOLVE_STATUS.get(error.code, 502)
        raise RegistrationError(error.code, message, status) from None


def resolve_youtube_channel_id(identifier):
    """resolve_youtube_channel のチャンネルIDだけが必要なとき用。"""
    return resolve_youtube_channel(identifier).channel_id


def resolve_twitch_user(identifier):
    """チャンネル名/URLを検証し、Twitch上に存在するユーザー(id, login, display_name)を返す。"""
    login = twitch_eventsub.parse_twitch_login(identifier)
    if not login:
        raise RegistrationError("invalid_input", "Twitchのチャンネル名またはURL（例: https://www.twitch.tv/名前）を指定してください。", 422)
    config_error = Config.twitch_config_error()
    if config_error:
        raise RegistrationError(config_error, describe_error(config_error), 503)
    try:
        user = twitch_api.get_user_by_login(login)
    except TwitchAPIError as error:
        logger.warning("[twitch_api] user lookup failed code=%s", error.code)
        raise RegistrationError("twitch_api_error", "Twitchチャンネルの確認に失敗しました: " + describe_error(error.code), 502) from None
    if not user or not user.get("id"):
        raise RegistrationError("not_found", "Twitchチャンネル「%s」が見つかりません。チャンネル名を確認してください。" % login, 404)
    return user


def _guild_limit_message(current):
    return ("このサーバーで登録できるチャンネルは%d個までです（YouTube・Twitch合計、現在%d個）。"
            "/remove_youtube または /remove_twitch で不要な登録を削除してから、もう一度お試しください。"
            % (Config.MAX_CHANNELS_PER_GUILD, current))


def count_guild_channels(db, guild_id):
    """このサーバーが登録しているチャンネル数(YouTube・Twitch合計)。"""
    return db.query(Subscription).filter(Subscription.discord_guild_id == guild_id).count()


def check_guild_capacity(db, guild_id, platform, channel_id):
    """新規追加になるときだけ、1サーバーあたりの登録上限を確認する。

    既に同じサーバーで登録済みのチャンネル(通知先やメッセージの変更)は上限に関係なく通す。
    DBを触る前に呼ぶことで、上限超過のときに配信者行や購読を作らずに済ませる。
    """
    if guild_id is None:
        return
    already_registered = db.query(Subscription).filter(
        Subscription.platform == platform,
        Subscription.youtube_channel_id == channel_id,
        Subscription.discord_guild_id == guild_id,
    ).first()
    if already_registered is not None:
        return
    current = count_guild_channels(db, guild_id)
    if current >= Config.MAX_CHANNELS_PER_GUILD:
        raise RegistrationError("guild_limit_reached", _guild_limit_message(current), 409)


def known_channel_id(db, platform, identifier):
    """外部API(クオータ)を使わずに、入力が指すチャンネルIDを割り出す。分からなければNone。

    YouTube: UC...形式ならそのまま。ハンドルは変換キャッシュにある場合だけ分かる。
    Twitch: 登録済みのログイン名ならDBから引ける。
    """
    if platform == PLATFORM_YOUTUBE:
        channel_id = youtube_api.parse_youtube_channel_id(identifier)
        if channel_id:
            return channel_id
        return youtube_api.cached_channel_id(youtube_api.parse_youtube_handle(identifier))
    text = (identifier or "").strip()
    # 候補から選んだときに渡るのは broadcaster_user_id(数値)。ログイン名より先に照合する。
    if text.isdigit() and db.query(Broadcaster).filter(
            Broadcaster.platform == PLATFORM_TWITCH, Broadcaster.channel_id == text).first():
        return text
    login = twitch_eventsub.parse_twitch_login(text)
    if not login:
        return None
    b = db.query(Broadcaster).filter(Broadcaster.platform == PLATFORM_TWITCH,
                                     Broadcaster.twitch_login == login).first()
    return b.channel_id if b else None


def check_guild_capacity_before_lookup(platform, identifier, guild_id):
    """YouTube/TwitchのAPIを呼ぶ前に、上限超過が確実なら弾く(クオータを使わない)。

    上限に達しているサーバーでも、既に登録済みのチャンネルなら設定変更として通す。
    入力がどのチャンネルを指すか外部APIなしに分からない場合は、
    クオータを使わないことを優先して弾き、チャンネルIDでの指定を案内する。
    """
    if guild_id is None:
        return
    with SessionLocal() as db:
        current = count_guild_channels(db, guild_id)
        if current < Config.MAX_CHANNELS_PER_GUILD:
            return
        channel_id = known_channel_id(db, platform, identifier)
        if channel_id is not None:
            # 既存登録の更新なら通る。新規ならここで上限エラーになる。
            check_guild_capacity(db, guild_id, platform, channel_id)
            return
        raise RegistrationError("guild_limit_reached", _guild_limit_message(current) +
                                "（既に登録済みのチャンネルの設定を変更する場合は、"
                                "チャンネルID（UC...）を含むURLで指定してください）", 409)


def _upsert_destination(db, platform, channel_id, guild_id, discord_channel_id, custom_message):
    if guild_id is None or discord_channel_id is None:
        return None
    existing = db.query(Subscription).filter(
        Subscription.platform == platform,
        Subscription.youtube_channel_id == channel_id,
        Subscription.discord_guild_id == guild_id,
    ).first()
    if existing is None:
        check_guild_capacity(db, guild_id, platform, channel_id)
        db.add(Subscription(platform=platform, discord_guild_id=guild_id, discord_channel_id=discord_channel_id,
                            youtube_channel_id=channel_id, custom_message=custom_message))
        return "created"
    if existing.discord_channel_id == discord_channel_id and (not custom_message or existing.custom_message == custom_message):
        return "unchanged"
    # /add_youtube と同じく、同じサーバー内の再登録は通知先とメッセージの更新として扱う
    existing.discord_channel_id = discord_channel_id
    if custom_message:
        existing.custom_message = custom_message
    return "updated"


def _get_or_create_broadcaster(db, platform, channel_id, **fields):
    """戻り値: (broadcaster, created)。同時登録で一意制約に当たった場合は既存行を採用する。"""
    query = db.query(Broadcaster).filter(Broadcaster.platform == platform, Broadcaster.channel_id == channel_id)
    b = query.first()
    if b is not None:
        for key, value in fields.items():
            if value and getattr(b, key) != value:
                setattr(b, key, value)
        return b, False
    b = Broadcaster(platform=platform, channel_id=channel_id, **fields)
    db.add(b)
    try:
        db.flush()
        return b, True
    except IntegrityError:
        db.rollback()
        return query.one(), False


def register_twitch_channel(identifier, guild_id=None, discord_channel_id=None, custom_message=None):
    """Twitchチャンネルを解決→DB保存→EventSub購読作成まで行う。
    購読作成に失敗しても登録は残し(毎時の確認で再試行)、結果の subscription_error で知らせる。"""
    check_guild_capacity_before_lookup(PLATFORM_TWITCH, identifier, guild_id)
    user = resolve_twitch_user(identifier)
    user_id, login = str(user["id"]), user.get("login")
    with SessionLocal() as db:
        b, created = _get_or_create_broadcaster(
            db, PLATFORM_TWITCH, user_id, name=user.get("display_name") or login,
            twitch_login=login, twitch_broadcaster_user_id=user_id)
        destination = _upsert_destination(db, PLATFORM_TWITCH, user_id, guild_id, discord_channel_id, custom_message)
        db.commit()
        broadcaster_pk, name = b.id, b.name
    ensured = twitch_eventsub.ensure_subscription(broadcaster_pk, force=True)
    return RegistrationResult(broadcaster_pk, PLATFORM_TWITCH, user_id, name, login, created, destination,
                              ensured.status, ensured.error, ensured.transient)


def register_youtube_channel(identifier, guild_id=None, discord_channel_id=None, custom_message=None):
    """管理画面用。/add_youtube と同じく、購読(WebSub)の開始はCeleryへ依頼する。"""
    check_guild_capacity_before_lookup(PLATFORM_YOUTUBE, identifier, guild_id)
    channel_id, name = resolve_youtube_channel(identifier)
    with SessionLocal() as db:
        b, created = _get_or_create_broadcaster(db, PLATFORM_YOUTUBE, channel_id)
        if created:
            b.name = name or UNKNOWN_NAME
        elif name and not has_display_name(b):
            # ハンドル経由で名前が分かったので、Unknownのままの行を埋める
            b.name = name
        destination = _upsert_destination(db, PLATFORM_YOUTUBE, channel_id, guild_id, discord_channel_id, custom_message)
        db.commit()
        broadcaster_pk, name, state = b.id, b.name, b.subscription_state
    if created:
        try:
            request_initial_subscription.delay(broadcaster_pk)
        except Exception:
            logger.warning("Initial subscription enqueue failed count=1; hourly check will recover")
    return RegistrationResult(broadcaster_pk, PLATFORM_YOUTUBE, channel_id, name, None, created, destination,
                              state, None, False)


def has_display_name(b):
    """DBに表示できるチャンネル名が入っているか(登録直後は "Unknown" のことがある)。"""
    return bool(b is not None and b.name and b.name != UNKNOWN_NAME)


def _display_name(b, channel_id):
    """一覧・削除結果に出す名前。DBのチャンネル名を使い、無いときだけIDで代用する。"""
    if has_display_name(b):
        return b.name
    if b is not None and b.platform == PLATFORM_TWITCH and b.twitch_login:
        return b.twitch_login
    return channel_id


def list_guild_registrations(db, guild_id, platform):
    """このサーバーの登録一覧 [(channel_id, 表示名)] を返す(外部アクセスなし)。

    表示名はDBのチャンネル名。名前がまだ分かっていないものだけチャンネルIDで代用し、
    同じ名前が複数あるときは区別できるようにIDを添える。
    """
    subs = db.query(Subscription).filter(
        Subscription.discord_guild_id == guild_id, Subscription.platform == platform,
    ).order_by(Subscription.id).all()
    rows = [(sub.youtube_channel_id, _display_name(sub.broadcaster, sub.youtube_channel_id))
            for sub in subs]
    duplicated = {name for _, name in rows if [n for _, n in rows].count(name) > 1}
    return [(channel_id, "%s (%s)" % (name, channel_id) if name in duplicated and name != channel_id
             else name)
            for channel_id, name in rows]


def match_by_display_name(db, guild_id, platform, identifier):
    """表示名(チャンネル名)での指定を、このサーバーの登録の中から探す。

    Twitchの表示名はログイン名と違うことがあり("ボドカさん" と "vodkavdk" など)、
    識別子としては使えないため、削除対象を探すときだけこの照合を使う。
    複数一致したときは、取り違えを避けるため候補から選び直してもらう。
    """
    text = (identifier or "").strip().casefold()
    if not text:
        return None
    subs = db.query(Subscription).filter(
        Subscription.discord_guild_id == guild_id, Subscription.platform == platform).all()
    matches = set()
    for sub in subs:
        b = sub.broadcaster
        candidates = {_display_name(b, sub.youtube_channel_id)}
        if b is not None and b.twitch_login:
            candidates.add(b.twitch_login)
        if any(c and c.casefold() == text for c in candidates):
            matches.add(sub.youtube_channel_id)
    if len(matches) > 1:
        raise RegistrationError(
            "ambiguous_name",
            "「%s」に一致する登録が複数あります。候補から選び直してください。" % identifier.strip(), 409)
    return matches.pop() if matches else None


def unregister_channel(platform, identifier, guild_id):
    """このサーバーの通知登録を1件削除する。外部API(クオータ)は使わない。

    配信者(Broadcaster)と購読そのものは残す。他のサーバーが同じチャンネルを
    登録している場合があるためで、通知が止まらない既存の自動削除と同じ粒度。
    """
    if guild_id is None:
        raise RegistrationError("invalid_input", "このコマンドはサーバー内で実行してください。", 422)
    with SessionLocal() as db:
        channel_id = known_channel_id(db, platform, identifier)
        if channel_id is None:
            # 識別子として解釈できない場合は、このサーバーの登録名との一致を探す
            channel_id = match_by_display_name(db, guild_id, platform, identifier)
        if channel_id is None:
            if platform == PLATFORM_TWITCH:
                message = ("「%s」に一致する登録が見つかりません。"
                           "チャンネル名・ログイン名・URLのいずれかを指定してください"
                           "（候補から選ぶと確実です）。" % (identifier or "").strip())
            else:
                message = ("「%s」に一致する登録が見つかりません。"
                           "チャンネル名、またはチャンネルID（UCから始まる24文字）を含むURLを"
                           "指定してください（候補から選ぶと確実です）。" % (identifier or "").strip())
            raise RegistrationError("invalid_input", message, 422)
        sub = db.query(Subscription).filter(
            Subscription.platform == platform,
            Subscription.youtube_channel_id == channel_id,
            Subscription.discord_guild_id == guild_id,
        ).first()
        if sub is None:
            raise RegistrationError(
                "not_registered",
                "このサーバーには登録されていないチャンネルです（%s）。" % channel_id, 404)
        b = sub.broadcaster
        name = _display_name(b, channel_id)
        login = b.twitch_login if b is not None else None
        db.delete(sub)
        db.commit()
        remaining = count_guild_channels(db, guild_id)
    logger.info("Registration removed platform=%s guild=%s count=1", platform, guild_id)
    return RemovalResult(platform, channel_id, name, login, remaining)
