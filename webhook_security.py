import hmac
import hashlib

from config import Config

# ハブが使うハッシュ方式。YouTube は sha1 を送ってくるが、仕様上は sha256 等も
# ありうるため、受け取った method 名で切り替える。
_ALGORITHMS = {
    "sha1": hashlib.sha1,
    "sha256": hashlib.sha256,
    "sha384": hashlib.sha384,
    "sha512": hashlib.sha512,
}


def verify_signature(body: bytes, signature: str) -> bool:
    """YouTubeからの通知が偽物じゃないかチェックする"""
    if not signature:
        return False

    # ★従来は signature.split("=") で2分割していたため、"=" を含まない不正な
    #   ヘッダを送られると ValueError になり500を返していた。
    #   partition なら区切りが無くても必ず3つ組が返るので落ちない。
    method, sep, value = signature.partition("=")
    if not sep or not value:
        return False

    algorithm = _ALGORITHMS.get(method.strip().lower())
    if algorithm is None:
        # 未知のハッシュ方式は検証できない＝信用しない
        return False

    expected = hmac.new(
        Config.YOUTUBE_WEBHOOK_SECRET.encode(),
        body,
        algorithm,
    ).hexdigest()

    return hmac.compare_digest(value, expected)
