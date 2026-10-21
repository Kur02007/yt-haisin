import hmac

from fastapi import Header, HTTPException

from config import Config


def require_admin_api_key(x_api_key: str = Header(default=None)) -> None:
    """管理API全体に適用する認証依存関係。
    ADMIN_API_KEY未設定の場合は誤って無認証公開しないよう、常に拒否する(fail-closed)。"""
    if not Config.ADMIN_API_KEY:
        raise HTTPException(status_code=503, detail="Admin API is not configured")
    if not x_api_key or not hmac.compare_digest(x_api_key, Config.ADMIN_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
