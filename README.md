# Discord Stream Notification Bot

YouTubeおよびTwitchの配信開始をリアルタイムで検知し、Discordチャンネルへ通知を送信するボットシステムです。

## 概要

本プロジェクトは、外部プラットフォーム（YouTube, Twitch）からのWebhook（PubSubHubbub / EventSub）を受信し、Discordへ通知を配信する非同期処理ベースのWebアプリケーション・ボットです。大量のI/O待ちが発生する処理を効率的に捌くため、非同期フレームワークとタスクキューを採用しています。

## 主な機能

* **リアルタイム通知**: YouTubeおよびTwitchの配信開始・終了イベントをWebhookで受け取り、遅延なくDiscordに通知
* **スラッシュコマンド対応**: `/add_youtube`, `/add_twitch` などの直感的なUIで購読設定を管理
* **バックグラウンド処理**: 通知の送信や購読の自動更新など、時間のかかる処理をCeleryで非同期化
* **多言語対応 (i18n)**: サーバーごとの言語設定に応じたメッセージ配信

## 技術スタック

* **言語**: Python 3.9
* **Webフレームワーク**: FastAPI / Uvicorn
* **Discord API**: discord.py
* **データベース・ORM**: PostgreSQL / SQLAlchemy
* **タスクキュー・インメモリDB**: Celery / Redis
* **インフラ**: Docker / Docker Compose

## 技術選定の背景

### 1. なぜ FastAPI なのか？
本システムは、外部API（YouTube/Twitch）からのWebhook受信エンドポイントや、Discord APIへのリクエスト、DBアクセスなど、**通信待ち（I/Oバウンド）**が多く発生するアーキテクチャです。
同期的なフレームワークではリクエストが詰まる（スレッドがブロックされる）懸念があったため、`async/await` による非同期処理を標準サポートし、高いスループットを維持できる **FastAPI** を採用しました。

### 2. Pydantic による堅牢な型定義
Pythonの柔軟性を活かしつつ、予期せぬエラーを防ぐために **Pydantic** を活用しています。Webhookで送られてくる複雑なJSONデータに対して、フレームワークの入口で厳格なバリデーション（型定義と検証）を行うことで、安全性の高いシステムを実現しています。

## ディレクトリ構成（例）

```text
.
├── main.py              # FastAPIサーバー（Webhook受信・ルーティング）
├── bot.py               # Discord Bot本体（スラッシュコマンド処理）
├── admin_app.py         # 管理用APIダッシュボード
├── models.py            # SQLAlchemyのDBモデル定義
├── schemas.py           # Pydanticの型定義・バリデーション
├── tasks.py             # Celeryのバックグラウンドタスク
├── docker-compose.yml   # コンテナ構成管理
└── .env.example         # 環境変数のサンプル
```

## 起動方法（ローカル環境）

### 前提条件
* Docker および Docker Compose がインストールされていること。

### セットアップ手順

1. **リポジトリのクローン**
   ```bash
   git clone https://github.com/<your-username>/<repository-name>.git
   cd <repository-name>
   ```

2. **環境変数の設定**
   `.env.example` をコピーして `.env` を作成し、必要なトークンやキーを入力します。
   ```bash
   cp .env.example .env
   ```

3. **コンテナのビルドと起動**
   ```bash
   docker-compose up -d --build
   ```

4. **動作確認**
   * BotがDiscordサーバーでオンラインになることを確認します。
   * FastAPIの自動生成ドキュメント（Swagger UI）: `http://localhost:8000/docs`