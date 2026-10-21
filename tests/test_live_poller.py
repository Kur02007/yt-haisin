"""ライブポーリングが他チャンネルの動画を通知しないことのテスト(外部通信なし)。"""
import os
os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("CELERY_BROKER_URL", "memory://")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test")
os.environ.setdefault("YOUTUBE_WEBHOOK_SECRET", "test-secret")
os.environ.setdefault("CALLBACK_URL", "https://example.invalid/webhook/youtube")
import unittest
from unittest.mock import Mock, patch

import live_poller
import polling_tasks
from models import Base, engine

CHANNEL_ID = "UCyi77gpUUBatJcUT1JynxkA"
OTHER_CHANNEL_ID = "UCy5A4S8tY0pxvzRhYrxodwg"


def page(html):
    return Mock(status_code=200, text=html)


class LivePageTest(unittest.TestCase):
    def test_ignores_video_ids_without_watch_canonical(self):
        # 配信中でないページでも、おすすめ欄の "isLive" や videoId は混ざりうる
        html = ('<link rel="canonical" href="https://www.youtube.com/channel/%s">'
                '"isLive":true "videoId":"wTM8pprrBQI"' % CHANNEL_ID)
        with patch.object(live_poller.requests, "get", return_value=page(html)):
            self.assertIsNone(live_poller.get_current_live_video_id(CHANNEL_ID))

    def test_uses_watch_canonical(self):
        html = ('<link rel="canonical" href="https://www.youtube.com/watch?v=MTtV4V6UfF8">'
                '"isLive":true')
        with patch.object(live_poller.requests, "get", return_value=page(html)):
            self.assertEqual(live_poller.get_current_live_video_id(CHANNEL_ID), "MTtV4V6UfF8")


class PollChannelTest(unittest.TestCase):
    def setUp(self):
        Base.metadata.create_all(engine)
        self.addCleanup(Base.metadata.drop_all, engine)
        for target, value in [("get_current_live_video_id", "wTM8pprrBQI"),
                              ("get_best_thumbnail", "thumb")]:
            p = patch.object(polling_tasks, target, return_value=value)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(polling_tasks.process_livestream_task, "delay")
        self.enqueue = p.start()
        self.addCleanup(p.stop)

    def poll(self, snippet):
        with patch.object(polling_tasks, "fetch_video_details", return_value={"snippet": snippet}):
            return polling_tasks.poll_channel_live_task.run(CHANNEL_ID)

    def test_rejects_video_of_other_channel(self):
        result = self.poll({"channelId": OTHER_CHANNEL_ID, "channelTitle": "だいじろー Daijiro",
                            "liveBroadcastContent": "live"})
        self.assertEqual(result, "foreign_video")
        self.enqueue.assert_not_called()

    def test_rejects_non_live_video(self):
        result = self.poll({"channelId": CHANNEL_ID, "liveBroadcastContent": "upcoming"})
        self.assertEqual(result, "not_live")
        self.enqueue.assert_not_called()

    def test_dispatches_own_live(self):
        result = self.poll({"channelId": CHANNEL_ID, "channelTitle": "千笑乃まこ🐶🎮VTuber",
                            "title": "配信", "liveBroadcastContent": "live"})
        self.assertEqual(result, "dispatched")
        self.assertEqual(self.enqueue.call_args[0][0]["author_name"], "千笑乃まこ🐶🎮VTuber")


if __name__ == "__main__":
    unittest.main()
