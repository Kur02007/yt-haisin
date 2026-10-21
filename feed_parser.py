from typing import Optional

import feedparser


class FeedParseError(Exception):
    """YouTubeのAtomフィードに想定タグ(yt_videoid等)が無かった場合に送出する"""


def parse_video_entry(xml_data: str) -> Optional[dict]:
    """PubSubHubbubで届いたAtomフィードから、通知パイプライン用のvideo_dataを取り出す。
    更新エントリが無い(削除通知など)場合はNoneを返す。"""
    feed = feedparser.parse(xml_data)
    if not feed.entries:
        return None

    entry = feed.entries[0]

    # YouTubeのXMLには "yt_videoid" や "yt_channelid" という特殊なタグがある
    try:
        return {
            "channel_id": entry.yt_channelid,
            "video_id": entry.yt_videoid,
            "title": entry.title,
            "link": entry.link,
            "published": entry.published,
            # サムネイルはAPIを使わずにURL生成できる
            "thumbnail_url": f"https://img.youtube.com/vi/{entry.yt_videoid}/maxresdefault.jpg",
            "author_name": entry.author_detail.name if 'author_detail' in entry else "Unknown",
        }
    except AttributeError as e:
        raise FeedParseError(str(e)) from e
