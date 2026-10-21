import time
import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from celery_app import celery_app
from discord_notifier import (
    SEND_CHANNEL_MISSING,
    SEND_OK,
    send_discord_message,
    send_followup_members_only_message,
)
from i18n import DEFAULT_LANGUAGE, get_guild_languages
from models import PLATFORM_YOUTUBE, Broadcaster, SessionLocal, LiveStream, Subscription
from thumbnails import get_best_thumbnail
from youtube_api import classify_broadcast, fetch_video_details

# ライブ配信開始通知の何分後にメンバー限定チェックを再確認するか。
# 実測: 開始9分後の公開配信でも viewCount に実数値が入ることを確認済み。
MEMBERS_ONLY_RECHECK_DELAY_SECONDS = 300

# 通知先チャンネルが見つからない(404 Unknown Channel)エラーが「連続で」この回数に達したら、
# その通知設定(Subscription)をDBから自動削除する。
# 単発の404で消さないのは、Discord側の一時的な不整合で消してしまうのを避けるため。
MAX_CHANNEL_MISSING_FAILURES = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _count_channel_missing(sub) -> bool:
    """宛先不明(404)の連続回数を1つ進める。自動削除すべきなら True を返す。

    連続回数は送信成功時に0へ戻すため、たまたま1回404になっただけの設定は消えない。
    削除後も /add_youtube で別の有効なチャンネルを指定すれば再登録できる
    （新しいSubscriptionが作られ、fail_countは0から始まる）。
    """
    sub.fail_count = (sub.fail_count or 0) + 1

    if sub.fail_count < MAX_CHANNEL_MISSING_FAILURES:
        logger.warning(
            f"[auto_cleanup:channel_missing_count] 通知先チャンネルが見つかりません"
            f"（{sub.fail_count}/{MAX_CHANNEL_MISSING_FAILURES}回連続）。"
            f"あと{MAX_CHANNEL_MISSING_FAILURES - sub.fail_count}回続くとこの通知設定を自動削除します。"
            f" guild_id={sub.discord_guild_id} channel_id={sub.discord_channel_id}"
            f" youtube_channel_id={sub.youtube_channel_id}"
        )
        return False

    logger.error(
        f"[auto_cleanup:subscription_removed] 通知先チャンネルが"
        f"{sub.fail_count}回連続で見つからなかったため、この通知設定を自動削除しました"
        f"（チャンネルの削除、またはBotの退出が原因と考えられます）。"
        f"再度通知を受け取るには /add_youtube で有効なチャンネルを指定して登録し直してください。"
        f" guild_id={sub.discord_guild_id} channel_id={sub.discord_channel_id}"
        f" youtube_channel_id={sub.youtube_channel_id}"
    )
    return True


def _update_broadcaster_name(db, broadcaster, author_name):
    """通知に含まれるチャンネル名をDBへ反映する(YouTube APIのクオータは使わない)。

    /add_youtube ではチャンネルIDしか分からず name="Unknown" で登録されるため、
    フィード(author)やポーリング(channelTitle)で判明した時点でここで埋める。
    改名にも追従する。表示用の情報なので、失敗しても通知処理は止めない。
    """
    author_name = (author_name or "").strip()  # 前後の空白が入っていることがある
    if broadcaster is None or not author_name or author_name == "Unknown":
        return
    if broadcaster.name == author_name:
        return
    try:
        broadcaster.name = author_name
        db.commit()
    except SQLAlchemyError:
        db.rollback()
        logger.warning("Broadcaster name update failed count=1")


@celery_app.task(bind=True, max_retries=3)
def process_livestream_task(self, video_data):
    """通知メインタスク。PubSubHubbub(main.py)とライブポーリング(polling_tasks.py)の
    どちらから来た通知も、ここで同じdedup/分類/送信パイプラインに合流する。"""

    db: Session = SessionLocal()
    try:
        video_id = video_data['video_id']
        channel_id = video_data['channel_id']
        title = video_data['title']

        # 管理画面でis_active=falseにされたチャンネルは通知しない。
        # PubSubHubbub/ライブポーリングどちらから来た通知でもここで一元的に弾く。
        # あえてLiveStream登録より前に判定し、再度有効化された場合に途中の配信を
        # 取りこぼさない(dedup登録していないので後続の通知/ポーリングで拾い直せる)。
        broadcaster = db.query(Broadcaster).filter(
            Broadcaster.platform == PLATFORM_YOUTUBE, Broadcaster.channel_id == channel_id
        ).first()
        if broadcaster is not None and broadcaster.is_active is False:
            return "Inactive channel, skipped"

        # 通知の中身から分かるチャンネル名をDBへ反映する(/remove_* の一覧などで使う)
        _update_broadcaster_name(db, broadcaster, video_data.get("author_name"))

        exists = db.query(LiveStream).filter(LiveStream.video_id == video_id).first()
        if exists:
            return "Duplicate"

        new_stream = LiveStream(video_id=video_id, title=title, published_at=datetime.utcnow())
        db.add(new_stream)
        db.commit()

        subs = db.query(Subscription).filter(
            Subscription.platform == PLATFORM_YOUTUBE, Subscription.youtube_channel_id == channel_id
        ).all()

        if not subs:
            return "No subscribers"

        # ★ここで最適なサムネイルを取得し直す
        best_thumbnail = get_best_thumbnail(video_id)
        video_data['thumbnail_url'] = best_thumbnail

        # YouTube Data API videos.list(動画IDにつきクオータ1)で、
        # プレミア公開/メンバー限定(ベストエフォート)/配信予定時刻を判定する
        classification = classify_broadcast(video_id, title)

        # 通知先サーバーの言語設定(/language)。custom_message が無いときの既定文言に使う。
        # 別セッションで引き、失敗時は全サーバー ja になるだけ(ここでリトライさせると
        # LiveStream登録済みのため "Duplicate" で抜けて通知が失われる)。
        guild_languages = get_guild_languages(sub.discord_guild_id for sub in subs)

        sent_count = 0
        removed_count = 0
        for sub in subs:
            result = send_discord_message(
                sub.discord_channel_id, video_data, classification, sub.custom_message,
                language=guild_languages.get(sub.discord_guild_id, DEFAULT_LANGUAGE),
            )
            if result == SEND_OK:
                sent_count += 1
                # 「連続」で数えるため、1回でも届いたらカウンタを0に戻す
                if sub.fail_count:
                    sub.fail_count = 0
            elif result == SEND_CHANNEL_MISSING:
                # 宛先が存在しない。規定回数連続したらこの通知設定を自動削除する
                if _count_channel_missing(sub):
                    db.delete(sub)
                    removed_count += 1
            time.sleep(0.1)

        # カウンタの増減と自動削除をまとめて確定する。
        # ここで失敗しても LiveStream は登録済みのため、リトライ時は "Duplicate" で
        # 抜けるだけで、同じ通知が二重に送られることはない。
        db.commit()

        # ライブ配信中(broadcast_state=="live")の場合のみ、少し待ってから
        # メンバー限定判定を再確認する。upcoming/premiere/videoは対象外
        # (video/premiereは通知時点で既にviewCount等から判定済み、upcomingは
        # まだ配信が始まっておらず再生数が育たないため待っても無意味なので)。
        if classification['broadcast_state'] == 'live':
            verify_members_only_task.apply_async(
                args=[
                    video_id,
                    channel_id,
                    title,
                    video_data['link'],
                    video_data['author_name'],
                    classification['is_members_only'],
                ],
                countdown=MEMBERS_ONLY_RECHECK_DELAY_SECONDS,
            )

        if removed_count:
            return f"Sent {sent_count} notifications (removed {removed_count} dead subscriptions)"
        return f"Sent {sent_count} notifications"

    except SQLAlchemyError as e:
        # DB接続/クエリの失敗＝こちら側のシステム内部エラー（インフラ要因の可能性が高い）
        logger.error(
            f"[system_error:db] DB操作に失敗しました (video_id={video_data.get('video_id')}): {e}"
        )
        raise self.retry(exc=e, countdown=60)
    except Exception as e:
        # 想定外の内部エラー（Discord送信自体の失敗は send_discord_message 側で
        # クライアント設定ミスかどうかを判別済みなので、ここには上がってこない）
        logger.error(
            f"[system_error:unknown] 予期しない内部エラーが発生しました (video_id={video_data.get('video_id')}): {e}"
        )
        raise self.retry(exc=e, countdown=60)
    finally:
        db.close()


@celery_app.task(bind=True, max_retries=2)
def verify_members_only_task(self, video_id, channel_id, title, link, author_name, already_flagged):
    """ライブ配信開始通知の約5分後に再度videos.listを呼び、
    statistics.viewCountの欠落(実測ベースの未文書化シグナル)からメンバー限定を再判定する。
    開始時点の通知で既にメンバー限定と判定済みなら何もしない。
    新たにメンバー限定と判明した場合のみ、追記メッセージを送る(誤検知防止のため
    「公開のまま」の場合は何も送らない＝通知を増やしすぎない設計)。"""
    try:
        api_details = fetch_video_details(video_id)
        if already_flagged or not api_details:
            return "Skip: already flagged or API check unavailable"

        statistics = api_details.get("statistics")
        view_count_hidden = statistics is not None and "viewCount" not in statistics
        logger.info(
            f"[youtube_api] members_only recheck video_id={video_id} "
            f"view_count_hidden={view_count_hidden}"
        )
        if not view_count_hidden:
            return "viewCount present: likely public, no follow-up sent"

        db = SessionLocal()
        try:
            subs = db.query(Subscription).filter(
                Subscription.platform == PLATFORM_YOUTUBE, Subscription.youtube_channel_id == channel_id
            ).all()
        finally:
            db.close()

        if not subs:
            return "No subscribers"

        data = {"author_name": author_name, "title": title, "link": link}
        guild_languages = get_guild_languages(sub.discord_guild_id for sub in subs)
        sent_count = 0
        for sub in subs:
            success = send_followup_members_only_message(
                sub.discord_channel_id, data,
                language=guild_languages.get(sub.discord_guild_id, DEFAULT_LANGUAGE),
            )
            if success:
                sent_count += 1
            time.sleep(0.1)

        return f"Sent {sent_count} follow-up notices"

    except SQLAlchemyError as e:
        logger.error(f"[system_error:db] メンバー限定再確認でDB操作に失敗しました (video_id={video_id}): {e}")
        raise self.retry(exc=e, countdown=60)
    except Exception as e:
        # あくまで補助機能のため、失敗しても通知そのものには影響させない(retryのみ、警告扱い)
        logger.warning(f"メンバー限定再確認タスクで予期しないエラー (video_id={video_id}): {e}")


@celery_app.task
def cleanup_old_logs():
    db = SessionLocal()
    try:
        cutoff = datetime.utcnow() - timedelta(days=30)
        db.query(LiveStream).filter(LiveStream.created_at < cutoff).delete()
        db.commit()
    finally:
        db.close()
