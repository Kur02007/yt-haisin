# (1) 標準ライブラリ
import asyncio
import re

# (2) 外部ライブラリ
import discord
from discord import app_commands
from discord.ext import commands
import logging
from sqlalchemy.exc import SQLAlchemyError

# (3) 自作モジュール
from config import Config
import channel_registration
from channel_registration import (UNKNOWN_NAME, RegistrationError, check_guild_capacity,
                                  check_guild_capacity_before_lookup, describe_error,
                                  has_display_name, list_guild_registrations,
                                  parse_youtube_handle, register_twitch_channel,
                                  resolve_youtube_channel, unregister_channel)
from twitch_eventsub import parse_twitch_login
from i18n import COMMAND_TEXTS, DEFAULT_LANGUAGE, get_guild_language, set_guild_language, t
from subscription_tasks import request_initial_subscription
from models import PLATFORM_TWITCH, PLATFORM_YOUTUBE, SessionLocal, Broadcaster, Subscription, init_db

logger = logging.getLogger(__name__)

# ★root に DiscordErrorNotifier を足すと logging.lastResort が使われなくなり、
#   このファイルの logger.* が docker logs から完全に消える（DMにしか飛ばない）。
#   購読の成否を後から追えるよう、標準エラーへの出力をここで確保しておく。
logger.setLevel(logging.INFO)
_stderr_handler = logging.StreamHandler()
_stderr_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
)
logger.addHandler(_stderr_handler)

intents = discord.Intents.default()
intents.message_content = True

# Discordクライアントの表示言語 → i18n の言語コード。ここに無い言語(ja を含む)は日本語の原文のまま。
# 繁体字(zh-TW)にも簡体字の訳を出す。
_COMMAND_LOCALES = {
    discord.Locale.american_english: "en",
    discord.Locale.british_english: "en",
    discord.Locale.korean: "ko",
    discord.Locale.chinese: "zh",
    discord.Locale.taiwan_chinese: "zh",
}


class CommandTranslator(app_commands.Translator):
    """スラッシュコマンドの説明文を、見る人のDiscordの表示言語に合わせる(i18n.COMMAND_TEXTS)。
    コマンド名・引数名・選択肢は翻訳しない(どの言語でも /add_youtube などで通じるように)。"""

    async def translate(self, string: app_commands.locale_str, locale: discord.Locale,
                        context: app_commands.TranslationContext):
        if context.location not in (app_commands.TranslationContextLocation.command_description,
                                    app_commands.TranslationContextLocation.parameter_description):
            return None
        language = _COMMAND_LOCALES.get(locale)
        if language is None:
            return None
        return COMMAND_TEXTS.get(string.message, {}).get(language)


class YouTubeBot(commands.Bot):
    def __init__(self, **kwargs):
        super().__init__(command_prefix='/', intents=intents, **kwargs)

    async def setup_hook(self):
        # 1. DBの初期化
        init_db()
        # 2. コマンドの同期(説明文の各言語版も一緒に登録される)
        await self.tree.set_translator(CommandTranslator())
        await self.tree.sync()
        
        # 3. 既存のログ監視を開始する。購読処理はCeleryに一本化。
        handler = DiscordErrorNotifier(self)
        handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
        logging.getLogger().addHandler(handler) # 全体のログを監視対象にする
            
        logger.info("✅ Slash commands synced")

    async def update_guild_count_status(self):
        """ステータス欄に導入サーバー数を出す。起動時とサーバー参加・脱退のたびに更新する。"""
        count = len(self.guilds)
        await self.change_presence(activity=discord.CustomActivity(name=f"{count}サーバーで稼働中"))
        logger.info("Guild count status updated: %d", count)

    async def on_ready(self):
        # 再接続のたびにも呼ばれるが、そのつど数え直すだけなので問題ない
        await self.update_guild_count_status()

    async def on_guild_join(self, guild):
        await self.update_guild_count_status()

    async def on_guild_remove(self, guild):
        await self.update_guild_count_status()

bot = YouTubeBot()

# /add_youtube・/add_twitch の新規登録成功メッセージを自動で消すまでの秒数
SUCCESS_MESSAGE_DELETE_SECONDS = 10

def extract_channel_id(url: str):
    """UC...形式のチャンネルIDだけを取り出す(外部アクセスなし)。ハンドルURLはNoneになる。"""
    return channel_registration.parse_youtube_channel_id(url)


def _registration_error_text(language, error: RegistrationError, url: str, platform=PLATFORM_YOUTUBE):
    """/add_youtube・/add_twitch の RegistrationError を利用者の言語で返す。
    ja は channel_registration 側の文言をそのまま使う(従来どおり。管理画面とも共通)。"""
    if language == DEFAULT_LANGUAGE:
        return f"❌ {error.message}"
    if error.code == "guild_limit_reached":
        return t(language, "error_guild_limit_reached", max=Config.MAX_CHANNELS_PER_GUILD)
    if platform == PLATFORM_TWITCH:
        if error.code == "invalid_input":
            return t(language, "error_twitch_invalid_input")
        if error.code == "not_found":
            return t(language, "error_twitch_not_found", login=parse_twitch_login(url) or url)
        if error.code.startswith("twitch_") and error.code != "twitch_api_error":
            # twitch_not_configured など、運営側の設定不備
            return t(language, "error_twitch_not_configured", code=error.code)
        return t(language, "error_twitch_api", code=error.code)
    if error.code == "not_found":
        return t(language, "error_not_found", handle=parse_youtube_handle(url) or url)
    if error.code in ("invalid_input", "not_configured", "quota_exceeded"):
        return t(language, f"error_{error.code}")
    return t(language, "error_lookup_failed", code=error.code)


def _removal_error_text(language, error: RegistrationError, identifier: str, platform: str):
    """/remove_* の RegistrationError を利用者の言語で返す(ja は従来の文言)。"""
    if language == DEFAULT_LANGUAGE:
        return f"❌ {error.message}"
    identifier = (identifier or "").strip()
    if error.code == "invalid_input":
        return t(language, f"error_remove_no_match_{platform}", identifier=identifier)
    if error.code in ("not_registered", "ambiguous_name"):
        return t(language, f"error_{error.code}", identifier=identifier)
    return t(language, "unexpected_error")


async def _interaction_language(interaction: discord.Interaction):
    """サーバー内ならそのサーバーの言語設定、DMなら利用者のDiscordの表示言語。"""
    if interaction.guild_id is None:
        return _COMMAND_LOCALES.get(interaction.locale, DEFAULT_LANGUAGE)
    return await asyncio.to_thread(get_guild_language, interaction.guild_id)


@bot.tree.command(name="language", description="Botの応答・通知の言語を設定します")
@app_commands.describe(language="言語")
@app_commands.choices(language=[
    app_commands.Choice(name="日本語 (ja)", value="ja"),
    app_commands.Choice(name="English (en)", value="en"),
    app_commands.Choice(name="한국어 (ko)", value="ko"),
    app_commands.Choice(name="中文 (zh)", value="zh"),
])
@app_commands.guild_only()
async def set_language(interaction: discord.Interaction, language: app_commands.Choice[str]):
    try:
        saved = await asyncio.to_thread(set_guild_language, interaction.guild_id, language.value)
    except SQLAlchemyError as e:
        logger.error(f"[system_error:db] 言語設定の保存に失敗しました (guild={interaction.guild_id}): {e}")
        await interaction.response.send_message(t(language.value, "system_error"), ephemeral=True)
        return
    await interaction.response.send_message(t(saved, "language_set"))


@set_language.error
async def language_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    await add_twitch_error(interaction, error)


@bot.tree.command(name="add_youtube", description="YouTubeチャンネルの通知を登録・更新します")
@app_commands.describe(url="チャンネルURL・ハンドルURL(@name)・チャンネルID", channel="通知先チャンネル", message="通知メッセージ")
async def add_youtube(
    interaction: discord.Interaction, 
    url: str, 
    channel: discord.TextChannel = None,
    message: str = None
):
    await interaction.response.defer()

    # ★現在のサーバーIDを取得
    current_guild_id = interaction.guild_id
    # 応答はサーバーの言語設定(/language)で返す。未設定・DMは日本語。
    lang = await asyncio.to_thread(get_guild_language, current_guild_id)

    target_channel_id = channel.id if channel else interaction.channel_id
    target_channel_name = channel.mention if channel else t(lang, "this_channel")

    def resolve_with_limit_check():
        # 上限に達していれば、YouTube APIを呼ぶ前(クオータを使う前)に断る。
        check_guild_capacity_before_lookup(PLATFORM_YOUTUBE, url, current_guild_id)
        # チャンネルID URLはそのまま、ハンドルURL(@name)はYouTube APIで1回だけ引く
        # (同じ1クオータでチャンネル名も分かる。IDで指定された場合は名前は不明)。
        return resolve_youtube_channel(url)

    try:
        # DBアクセスとネットワークI/Oを含むのでイベントループを止めないよう別スレッドで実行する。
        youtube_id, resolved_name = await asyncio.to_thread(resolve_with_limit_check)
    except RegistrationError as e:
        await interaction.followup.send(_registration_error_text(lang, e, url))
        return

    db = SessionLocal()
    try:
        # 1サーバーあたりの登録上限(YouTube・Twitch合計)。
        # 配信者行やWebSub購読を作る前に確かめ、上限超過なら何も残さない。
        check_guild_capacity(db, current_guild_id, PLATFORM_YOUTUBE, youtube_id)

        broadcaster = db.query(Broadcaster).filter(
            Broadcaster.platform == PLATFORM_YOUTUBE, Broadcaster.channel_id == youtube_id
        ).first()
        if not broadcaster:
            # 実際のPOST受付時刻はCeleryタスクだけが記録する。
            # 名前が分からない場合は、最初の通知が届いた時点でフィードから埋まる。
            broadcaster = Broadcaster(
                channel_id=youtube_id, 
                name=resolved_name or UNKNOWN_NAME
            )
            db.add(broadcaster)
            db.commit()
            try:
                request_initial_subscription.delay(broadcaster.id)
            except Exception:
                # 毎時タスクがunknown状態を回収する。登録UIの処理は継続する。
                logger.warning("Initial subscription enqueue failed count=1; hourly check will recover")
            
        elif resolved_name and not has_display_name(broadcaster):
            # ハンドルURLで名前が分かったので、Unknownのままの既存行を埋める
            broadcaster.name = resolved_name
            db.commit()

        # ★【重要変更】「このYouTubeID」かつ「このサーバー(Guild)」の登録を探す
        existing = db.query(Subscription).filter(
            Subscription.platform == PLATFORM_YOUTUBE,
            Subscription.youtube_channel_id == youtube_id,
            Subscription.discord_guild_id == current_guild_id
        ).first()

        if existing:
            # 同じサーバー内での更新（チャンネル移動やメッセージ変更）
            existing.discord_channel_id = target_channel_id
            if message:
                existing.custom_message = message
            db.commit()
            msg_status = t(lang, "message_set", message=message) if message else t(lang, "message_unchanged")
            await interaction.followup.send(t(lang, "youtube_updated", guild=interaction.guild.name,
                                              channel=target_channel_name, message_status=msg_status))
        else:
            # 新規登録（このサーバーでは初めて）
            new_sub = Subscription(
                discord_guild_id=current_guild_id, # ★サーバーIDを保存
                discord_channel_id=target_channel_id,
                youtube_channel_id=youtube_id,
                custom_message=message
            )
            db.add(new_sub)
            db.commit()
            msg_preview = message if message else t(lang, "default_message")
            sent = await interaction.followup.send(t(lang, "youtube_created", youtube_id=youtube_id,
                                                     channel=target_channel_name, message=msg_preview), wait=True)
            await sent.delete(delay=SUCCESS_MESSAGE_DELETE_SECONDS)

    except RegistrationError as e:
        # 登録上限など、利用者にそのまま見せられる理由(システムエラーではない)
        await interaction.followup.send(_registration_error_text(lang, e, url))
    except SQLAlchemyError as e:
        # DB接続/コミット失敗＝こちら側のシステム内部エラー
        logger.error(
            f"[system_error:db] DB操作に失敗しました (guild={current_guild_id}, youtube_id={youtube_id}): {e}"
        )
        await interaction.followup.send(t(lang, "system_error"))
    except Exception as e:
        # 想定外のエラー（コマンド引数の不備など、利用者側の入力起因の可能性を含む）
        logger.error(
            f"[unknown_error] コマンド処理中にエラーが発生しました (guild={current_guild_id}, youtube_id={youtube_id}): {e}"
        )
        await interaction.followup.send(t(lang, "unknown_error", error=str(e)))
    finally:
        db.close()
        

def _twitch_subscription_text(result, lang=DEFAULT_LANGUAGE):
    if result.subscription_status == "enabled":
        return t(lang, "eventsub_enabled")
    if result.subscription_status == "webhook_callback_verification_pending":
        return ""  # 数秒で有効になるため表示しない
    # 原因の説明文(describe_error)は日本語のみ。他の言語ではエラーコードを見せる。
    if lang == DEFAULT_LANGUAGE:
        reason = describe_error(result.subscription_error) or result.subscription_status
    else:
        reason = result.subscription_error or result.subscription_status
    return t(lang, "eventsub_failed", reason=reason)


@bot.tree.command(name="add_twitch", description="Twitchを追加：Twitchチャンネルの配信開始通知を登録・更新します")
@app_commands.describe(url="TwitchのチャンネルURLまたはチャンネル名", channel="通知先チャンネル", message="通知メッセージ")
@app_commands.guild_only()
async def add_twitch(
    interaction: discord.Interaction,
    url: str,
    channel: discord.TextChannel = None,
    message: str = None
):
    await interaction.response.defer()

    current_guild_id = interaction.guild_id
    lang = await asyncio.to_thread(get_guild_language, current_guild_id)
    target = channel or interaction.channel
    target_channel_name = channel.mention if channel else t(lang, "this_channel")

    # 通知を実際に送れるか(Botの権限)を先に確かめる。送れない登録は作らない。
    perms = target.permissions_for(interaction.guild.me)
    if not (perms.view_channel and perms.send_messages and perms.embed_links):
        await interaction.followup.send(t(lang, "twitch_no_permission", channel=target_channel_name))
        return

    try:
        # Twitch API呼び出しを含むため、イベントループを止めないよう別スレッドで実行する
        result = await asyncio.to_thread(register_twitch_channel, url, current_guild_id, target.id, message)
    except RegistrationError as e:
        await interaction.followup.send(_registration_error_text(lang, e, url, PLATFORM_TWITCH))
        return
    except SQLAlchemyError as e:
        logger.error(f"[system_error:db] Twitch登録のDB操作に失敗しました (guild={current_guild_id}): {e}")
        await interaction.followup.send(t(lang, "system_error"))
        return
    except Exception as e:
        logger.error(f"[unknown_error] Twitch登録中にエラーが発生しました (guild={current_guild_id}): {type(e).__name__}")
        await interaction.followup.send(t(lang, "unexpected_error"))
        return

    subscription_text = _twitch_subscription_text(result, lang)
    subscription_line = f"\n{subscription_text}" if subscription_text else ""
    channel_text = f"Twitch: **{result.name}** (`{result.login}`)"
    if result.destination == "unchanged":
        await interaction.followup.send(t(lang, "twitch_unchanged", twitch=channel_text,
                                          channel=target_channel_name, subscription=subscription_line))
    elif result.destination == "updated":
        msg_status = t(lang, "message_set", message=message) if message else t(lang, "message_unchanged")
        await interaction.followup.send(t(lang, "twitch_updated", guild=interaction.guild.name, twitch=channel_text,
                                          channel=target_channel_name, message_status=msg_status,
                                          subscription=subscription_line))
    else:
        msg_preview = message if message else t(lang, "default_message")
        sent = await interaction.followup.send(t(lang, "twitch_created", twitch=channel_text,
                                                 channel=target_channel_name, message=msg_preview,
                                                 subscription=subscription_line), wait=True)
        # 成功の案内はチャンネルに残さない。購読失敗の警告があるときは原因を読めるよう残す
        if not subscription_text.startswith("⚠️"):
            await sent.delete(delay=SUCCESS_MESSAGE_DELETE_SECONDS)


@add_twitch.error
async def add_twitch_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        lang = await _interaction_language(interaction)
    except Exception:
        lang = DEFAULT_LANGUAGE  # エラー応答そのものは必ず返す
    if isinstance(error, app_commands.NoPrivateMessage):
        text = "❌ このコマンドはサーバー内で実行してください。" if lang == DEFAULT_LANGUAGE else t(lang, "error_guild_only")
    else:
        logger.error(f"[unknown_error] /add_twitch の実行に失敗しました: {type(error).__name__}")
        text = t(lang, "unexpected_error")
    if interaction.response.is_done():
        await interaction.followup.send(text, ephemeral=True)
    else:
        await interaction.response.send_message(text, ephemeral=True)


def _fetch_registrations(guild_id, platform):
    with SessionLocal() as db:
        return list_guild_registrations(db, guild_id, platform)


async def _registered_choices(interaction: discord.Interaction, current: str, platform: str):
    """登録済みチャンネルを入力候補として出す(値はチャンネルID)。外部APIは使わない。"""
    if interaction.guild_id is None:
        return []
    try:
        rows = await asyncio.to_thread(_fetch_registrations, interaction.guild_id, platform)
    except Exception:
        # 候補が出せなくてもコマンド自体は手入力で使えるようにする(ここでERRORは出さない)
        logger.warning("Autocomplete lookup failed platform=%s count=1", platform)
        return []
    needle = (current or "").lower()
    choices = []
    for channel_id, label in rows:
        if needle and needle not in label.lower():
            continue
        choices.append(app_commands.Choice(name=label[:100], value=channel_id))
        if len(choices) >= 25:  # Discordの上限
            break
    return choices


async def _remove_registration(interaction: discord.Interaction, platform: str, identifier: str):
    await interaction.response.defer()
    lang = await asyncio.to_thread(get_guild_language, interaction.guild_id)
    try:
        result = await asyncio.to_thread(unregister_channel, platform, identifier, interaction.guild_id)
    except RegistrationError as e:
        await interaction.followup.send(_removal_error_text(lang, e, identifier, platform))
        return
    except SQLAlchemyError as e:
        logger.error(f"[system_error:db] 登録削除のDB操作に失敗しました "
                     f"(guild={interaction.guild_id}, platform={platform}): {e}")
        await interaction.followup.send(t(lang, "system_error"))
        return
    except Exception as e:
        logger.error(f"[unknown_error] 登録削除中にエラーが発生しました "
                     f"(guild={interaction.guild_id}, platform={platform}): {type(e).__name__}")
        await interaction.followup.send(t(lang, "unexpected_error"))
        return

    label = f"**{result.name}**" + (f" (`{result.login}`)" if result.login else f" (`{result.channel_id}`)")
    sent = await interaction.followup.send(t(lang, "removed", label=label, remaining=result.remaining,
                                             max=Config.MAX_CHANNELS_PER_GUILD), wait=True)
    await sent.delete(delay=SUCCESS_MESSAGE_DELETE_SECONDS)


@bot.tree.command(name="remove_youtube", description="YouTubeチャンネルの通知登録をこのサーバーから削除します")
@app_commands.describe(channel="登録済みチャンネル（候補から選択、またはチャンネルID・URL）")
@app_commands.guild_only()
async def remove_youtube(interaction: discord.Interaction, channel: str):
    await _remove_registration(interaction, PLATFORM_YOUTUBE, channel)


@remove_youtube.autocomplete("channel")
async def remove_youtube_autocomplete(interaction: discord.Interaction, current: str):
    return await _registered_choices(interaction, current, PLATFORM_YOUTUBE)


@bot.tree.command(name="remove_twitch", description="Twitchチャンネルの通知登録をこのサーバーから削除します")
@app_commands.describe(channel="登録済みチャンネル（候補から選択、またはチャンネル名・URL）")
@app_commands.guild_only()
async def remove_twitch(interaction: discord.Interaction, channel: str):
    await _remove_registration(interaction, PLATFORM_TWITCH, channel)


@remove_twitch.autocomplete("channel")
async def remove_twitch_autocomplete(interaction: discord.Interaction, current: str):
    return await _registered_choices(interaction, current, PLATFORM_TWITCH)


@remove_youtube.error
@remove_twitch.error
async def remove_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    await add_twitch_error(interaction, error)


        # あなたのDiscord ID
ADMIN_ID = 663280376483020831

class DiscordErrorNotifier(logging.Handler):
    def __init__(self, bot):
        super().__init__()
        self.bot = bot

    def emit(self, record):
        # ERROR 以上のレベル（ERROR, CRITICAL）が来たら発動
        if record.levelno >= logging.ERROR:
            err_msg = self.format(record)
            # 非同期でDMを送るタスクを作成
            self.bot.loop.create_task(self.send_error_to_admin(err_msg))

    async def send_error_to_admin(self, msg):
        try:
            admin = await self.bot.fetch_user(ADMIN_ID)
            # メッセージが長すぎるとDiscordで送れないのでカット
            short_msg = (msg[:1900] + '...') if len(msg) > 1900 else msg
            await admin.send(f"🚨 **デバッガー通知: エラーを検知しました**\n```py\n{short_msg}\n```")
        except Exception as e:
            print(f"Failed to send error DM: {e}")

if __name__ == "__main__":
    bot.run(Config.DISCORD_BOT_TOKEN)