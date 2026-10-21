from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

from config import Config

celery_app = Celery('tasks', broker=Config.CELERY_BROKER_URL)

celery_app.conf.beat_schedule = {
    'cleanup-old-logs': {
        'task': 'tasks.cleanup_old_logs',
        'schedule': crontab(hour=3, minute=0),
    },
    # ハブは 503 Transient error を数時間単位で返し続けることがある
    # （2026-09-07 は4時間以上ほぼ全滅）。3日おきの一発勝負では復旧の窓を
    # 逃すため毎時試行する。期限に余裕があるチャンネルはタスク側でスキップ
    # するので、実際にPOSTするのは期限が近いものだけ。
    'renew-subscriptions': {
        'task': 'subscription_tasks.renew_subscriptions',
        'schedule': crontab(minute=Config.SUBSCRIPTION_RENEWAL_SCHEDULE_MINUTE),
    },
    'urgent-subscriptions': {
        'task': 'subscription_tasks.urgent_subscriptions',
        'schedule': timedelta(minutes=Config.SUBSCRIPTION_URGENT_CHECK_MINUTES),
    },
    # 待機はワーカーを占有せずETA予約。再起動等で予約が消えてもDBから回収する。
    'due-subscription-checks': {
        'task': 'subscription_tasks.process_due_subscriptions',
        'schedule': timedelta(seconds=30),
    },
}

if Config.ENABLE_LIVE_POLLING:
    celery_app.conf.beat_schedule['poll-live-status'] = {
        'task': 'polling_tasks.poll_all_channels',
        'schedule': timedelta(seconds=Config.LIVE_POLL_INTERVAL_SECONDS),
    }

# Twitch EventSub購読の存在確認。失効(notification_failures_exceeded等)していれば作り直す。
# Twitch未設定の環境では登録しない(YouTubeのみの運用に影響させない)。
if Config.twitch_enabled():
    celery_app.conf.beat_schedule['reconcile-twitch-subscriptions'] = {
        'task': 'twitch_tasks.reconcile_twitch_subscriptions',
        'schedule': crontab(minute=Config.TWITCH_RECONCILE_SCHEDULE_MINUTE),
    }

# celery -A celery_app:celery_app worker/beat 起動時に各タスクモジュールを読み込ませるための指定。
# ここでは import せず名前だけ渡すことで、タスクモジュール側から celery_app への
# 一方向の依存関係を保ち、循環importを避けている。
celery_app.conf.imports = ('tasks', 'subscription_tasks', 'polling_tasks', 'twitch_tasks')
