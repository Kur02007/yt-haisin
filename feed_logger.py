import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# 受信した生XMLを保存するログファイル
RAW_FEED_LOG_FILE = "raw_feeds.log"
# 受信したフィードを構造化(JSON Lines)して保存するログファイル
STRUCTURED_FEED_LOG_FILE = "raw_feeds.jsonl"


def append_raw_feed_log(xml_data: str) -> None:
    """受信したXMLをそのままの形でログファイルに追記する"""
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        with open(RAW_FEED_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"----- {timestamp} -----\n")
            f.write(xml_data)
            f.write("\n")
    except OSError as e:
        logger.error(f"Failed to write raw feed log: {e}")


def append_structured_feed_log(record: dict) -> None:
    """受信したフィードの情報をJSON Lines形式（1行1レコード）で追記する"""
    record = {"timestamp": datetime.now(timezone.utc).isoformat(), **record}
    try:
        with open(STRUCTURED_FEED_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.error(f"Failed to write structured feed log: {e}")
