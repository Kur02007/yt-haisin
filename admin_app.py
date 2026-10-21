from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from admin_router import router as admin_router
from models import init_db

# 管理ダッシュボード専用のASGIアプリ。main.py(YouTube Webhook受信用)とは
# 別プロセス/別ポートで動かす想定(docker-compose.yml の admin サービスを参照)。
# 平文HTTPの公開ポートに認証キーを流したくないため、あえてアプリごと分離し、
# 127.0.0.1限定公開(SSHトンネル経由)にできるようにしている。
app = FastAPI(title="YouTube通知Bot 管理ダッシュボード")


@app.on_event("startup")
def on_startup():
    init_db()


app.include_router(admin_router)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
