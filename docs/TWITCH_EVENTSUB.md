# Twitch配信通知（EventSub）運用ガイド

YouTube通知に加えて、Twitchの配信開始（EventSub `stream.online`）をDiscordへ通知する機能の設定と運用手順です。
YouTube側の処理・コマンド・データはこれまでどおり動き、Twitchは設定したときだけ有効になります。

## 仕組み

```
Twitch ──(HTTPS POST, 署名付き)──> nginx:443 ──> web(8195) /webhook/twitch
                                                    │ 署名検証 → 10分以内か → メッセージID重複排除
                                                    │ → Celeryへ投入して即204応答
                                                    ▼
                                         worker: twitch_tasks.process_twitch_stream_online_task
                                           配信ID(twitch:<stream id>)で重複排除 → 配信タイトル取得 → Discordへ送信
```

- 購読するイベントは2種類です。`stream.online`（配信開始＝通知を送る）と `stream.offline`（配信終了＝通知のリンクをアーカイブへ差し替える）。1チャンネルにつきコストは合計2です。
- 購読の作成・確認：`/add_twitch`、管理画面での登録、web起動の30秒後、毎時37分の定期確認（`reconcile-twitch-subscriptions`）。
- 失効（`notification_failures_exceeded` など）：Twitchから届くrevocation通知をERRORログに記録し、5分後に自動で再購読します。毎時の確認でも作り直します。`user_removed`（配信者アカウントの削除）の場合は自動では再購読しません。
- 二重通知の防止：同じEventSubメッセージIDは1回だけ処理し（`twitch_eventsub_messages`、1日で削除）、同じ配信IDは `live_streams` で1回だけ通知します。

## 1. 必要な環境変数（`.env`）

| 変数 | 必須 | 内容 |
|---|---|---|
| `TWITCH_CLIENT_ID` | ○ | Twitch Developer ConsoleのClient ID |
| `TWITCH_CLIENT_SECRET` | ○ | 同 Client Secret |
| `TWITCH_WEBHOOK_SECRET` | ○ | 署名検証用のシークレット。**10〜100文字のASCII**。自分で生成する（例：`openssl rand -hex 32`） |
| `TWITCH_CALLBACK_URL` | ○ | Twitchから到達できる公開URL。例：`https://bot.example.com/webhook/twitch` |
| `TWITCH_RECONCILE_SCHEDULE_MINUTE` | | 毎時の購読確認を何分に行うか（既定値 `37`） |
| `TWITCH_REQUEST_TIMEOUT_SECONDS` | | Twitch APIのタイムアウト秒数（既定値 `10`） |

- 4つの必須変数が揃い、形式が正しいときだけTwitch機能が有効になります。未設定のときは `/webhook/twitch` が404を返し、定期確認もスケジュールされません。
- 値はソースコードやログに出しません。ログにはエラーコード（例：`eventsub_create_http_400`）とstatus名だけを出します。
- `.env` は全サービス（web / worker / beat / bot / admin）で共有されているので、1か所に書けば全サービスに反映されます。

## 2. Webhook URLの要件（重要）

Twitch EventSubのWebhookには、次の条件がすべて必要です。

- **HTTPS**であること（http不可）
- **443番ポート**であること（既存のYouTube用 `http://<IP>:8195` はそのままでは使えません）
- 公的に信頼される証明書であること（自己署名は不可）。そのため実質的に**ドメイン名**が必要です
- **Twitchのサーバーからインターネット経由で到達できる**こと。ファイアウォール、国外IPの制限、Basic認証などで遮断しないでください

このサーバーでは既にホストのnginxが443番で待ち受けているので、サイトを1つ追加してwebコンテナ（8195番）へ転送します。設定例：

```nginx
server {
    listen 443 ssl;
    server_name bot.example.com;                       # 実際のドメイン
    ssl_certificate     /etc/letsencrypt/live/bot.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/bot.example.com/privkey.pem;

    # Twitch EventSubだけを公開する（管理画面8196番は公開しない）
    location = /webhook/twitch {
        proxy_pass http://127.0.0.1:8195/webhook/twitch;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_read_timeout 10s;
        client_max_body_size 64k;
    }
    location / { return 404; }
}
```

- 署名はリクエストボディの生バイト列で検証するため、ボディを書き換える設定（gzip展開・文字コード変換など）は入れないでください。
- 反映：`nginx -t && systemctl reload nginx`。証明書は `certbot --nginx -d bot.example.com` などで取得します。
- 疎通確認：`curl -i -X POST https://bot.example.com/webhook/twitch` を実行して **403** が返れば、アプリまで届いています（署名がないので拒否されるのが正常です）。404なら `.env` のTwitch設定が不足しているか、webが再起動されていません。

## 3. Twitch Developer Console側の設定

1. https://dev.twitch.tv/console/apps でアプリを登録（取得済みなら不要）
   - OAuth Redirect URLs：`http://localhost` で構いません（Client Credentials Flowでは使いません）
   - Category：`Chat Bot` など任意
   - Client Type：**Confidential**（Client Secretを発行するため）
2. Client IDとClient Secretを `.env` に設定します
3. EventSubの購読はBotがAPIで自動作成するので、コンソールでの操作は不要です
   - `stream.online` は配信者の認可が不要なイベントで、アプリアクセストークン（Client Credentials）だけで購読できます
4. **同じClient IDを別のサービスと共用している場合**：同じ配信者の `stream.online` を別のコールバックURLで購読済みだと、Twitchが409を返して作成できません。その場合は登録結果に「別のコールバックURL宛てに既に存在します」と表示されます。Botは他サービスの購読を削除しません

## 4. 初回セットアップ・デプロイ手順

**順番が重要です。** 新しいコードは `platform` 列を前提にしているため、マイグレーションより先に再起動すると全サービスがDBエラーになります。

```bash
cd /root/yt-haisin-bot

# 1) バックアップ
docker exec yt-haisin-bot-db-1 pg_dump -U user -d youtube_bot_db > backup_$(date +%Y%m%d%H%M).sql

# 2) マイグレーション（稼働中の旧コードのままでも安全。旧コードのINSERTはDEFAULTでyoutubeになる）
docker exec -i yt-haisin-bot-db-1 psql -U user -d youtube_bot_db -v ON_ERROR_STOP=1 \
    < migrations/2026-09-22_add_platform_twitch.sql

# 3) 確認：既存行がすべて youtube になっていること
docker exec yt-haisin-bot-db-1 psql -U user -d youtube_bot_db \
    -c "SELECT platform, count(*) FROM broadcasters GROUP BY platform" \
    -c "SELECT platform, count(*) FROM subscriptions GROUP BY platform"

# 4) .env に TWITCH_* を追記（上の表を参照）

# 5) 全サービスを新コードで再起動（.:/app マウントのため再ビルドは不要だが、再起動は必須）
docker compose up -d --force-recreate web worker beat bot admin

# 6) nginxにサイトを追加して reload（2章）→ curlで403を確認
```

再起動後の確認：

- `docker compose logs beat | grep reconcile-twitch` … 毎時の確認が登録されていること
- Discordで `/add_twitch url:<チャンネル名>` を実行し、「新規登録しました」と表示されること（購読の作成に失敗したときだけ ⚠️ で原因が表示される）
- 数秒後、`docker compose logs web | grep "Twitch EventSub verification answered"` が出ること。管理画面の購読状態が「有効」になっていること

## 5. 配信終了後のアーカイブ差し替え

Twitchの配信ページ（`twitch.tv/<名前>`）は配信が終わると配信内容を指さなくなるため、配信終了時に**最初に送った通知メッセージの埋め込みを編集して、アーカイブ（VOD）のURLに差し替えます**。通知は増えず、メンションも再送されません。

- 対応づけは配信ID（`stream_id`）で行います。TwitchのVODは元の配信IDを持っているため、確実に同じ配信のアーカイブを特定できます。
- 差し替え後の埋め込み：タイトルが「【アーカイブ】…」になり、リンクとサムネイルがVODのもの、配信時間の欄と「配信終了」のフッターが付きます。
- 送信したメッセージは `twitch_stream_notifications` に記録します（30日で自動削除。差し替え済みの行は `archived=true`）。
- アーカイブがまだできていない場合は、2分後・10分後・30分後に再確認します。それでも見つからなければ、**元の通知をそのまま残して打ち切ります**（配信者が「過去の配信を保存」をオフにしている場合など）。
- 通知が削除済み・チャンネルが消えている場合は、その行だけ諦めます。
- アーカイブには保存期限（通常7日、アフィリエイト/パートナーやTurboで14〜60日）があり、期限後はTwitch側でリンク切れになります。

## 6. 使い方

### Discordコマンド

```
/add_twitch url:<TwitchのチャンネルURLまたはチャンネル名> [channel:<通知先>] [message:<通知メッセージ>]
```

- 表示名は「Twitchを追加」です。Discordのスラッシュコマンド名は小文字しか使えないため、`/add_Twitch` ではなく `/add_twitch` にしています。
- 引数は `/add_youtube` と同じ並び（`url`、`channel`、`message`）です。`url` には `twitchdev`、`@twitchdev`、`https://www.twitch.tv/twitchdev` のいずれも使えます。
- `/add_youtube` と同じく、実行者の権限は問いません（代理で設定できるようにするため）。Botが通知先チャンネルで「チャンネルを見る／メッセージを送信／埋め込みリンク」の権限を持っていることは事前に確認します。
- 同じサーバーでの再実行は `/add_youtube` と同じく、通知先・メッセージの更新になります。変更がなければ「既に登録済み」と表示されます。
- メッセージの `{name}` `{title}` `{link}` は、YouTubeと同じく置き換えられます。
- `/add_youtube` はこれまでどおりです（変更していません）。

### 管理画面

- 「チャンネル登録」でプラットフォーム（YouTube／Twitch）を選ぶと、入力欄とチェック内容が切り替わります。Twitchでは登録時にチャンネルの存在確認とEventSub購読の作成まで行います。
- 一覧にはプラットフォームのバッジと購読状態が表示されます。Twitchの行にある「再購読」を押すと、このBot宛ての購読を削除して作り直します（失効後の復旧や、シークレット変更後に使います）。
- 編集では表示名を変更できます（プラットフォームは変更できません）。

## 7. トラブルシューティング

| ログ・表示 | 意味と対処 |
|---|---|
| `Twitch EventSub subscription revoked status=notification_failures_exceeded` (ERROR) | Twitchからの通知に、こちらが一定回数応答できなかった。nginxや証明書、webが止まっていなかったか確認する。5分後と毎時の確認で自動的に再購読する |
| `status=webhook_callback_verification_failed` | 検証要求に正しく応答できなかった。URLに到達できるか（2章のcurl）と `TWITCH_WEBHOOK_SECRET` を確認し、管理画面の「再購読」を押す |
| `Twitch EventSub signature verification failed` (WARNING) | 署名が合わない要求を拒否した。単発なら無関係なアクセス。続く場合は `TWITCH_WEBHOOK_SECRET` を変更していないか確認する（変更したら全チャンネルで再購読が必要） |
| `ensure failed code=token_http_4xx` | Client ID／Secretが誤っているか、失効している |
| `ensure failed code=eventsub_create_http_400` | コールバックURLが要件（HTTPS・443・到達可能）を満たしていないか、シークレットの形式が不正 |
| `Twitch EventSub summary ...` (WARNING) | 毎時の確認で「有効」以外の購読があった。内訳のstatus名を見て上の各行で対処する |
| `status=user_removed` | 配信者のアカウントが削除または凍結された。復活した場合は `/add_twitch` を再実行するか「再購読」を押す |
| `[twitch_archive] アーカイブが見つかりませんでした` (INFO) | 配信者が過去の配信を保存していない可能性が高い。通知は元のリンクのまま残る（異常ではない） |
| `[twitch_archive] アーカイブリンクへの差し替えに失敗しました` (WARNING) | Discord APIがエラーを返した。次の配信終了時に再試行される |

### シークレット・コールバックURLを変更したとき

既存の購読は古い値のまま残ります（毎時の確認は、有効な購読があれば作り直しません）。

- **`TWITCH_WEBHOOK_SECRET` を変更した場合**：全サービスを再起動したあと、管理画面で各Twitchチャンネルの「再購読」を押してください。押さなくても、古い購読はTwitchから `notification_failures_exceeded` で失効させられ、その後に自動で作り直されますが、それまでの通知は届きません。
- **`TWITCH_CALLBACK_URL` を変更した場合**：古いURL宛ての購読は、Botからは他サービスの購読と区別できないため自動では削除しません。また、残っているとTwitchが409を返して新しいURLで作成できません。再起動後、次のコマンドで古いURL宛ての購読を削除してから「再購読」を押してください。

```bash
docker compose exec worker python -c "
import twitch_api
from models import SessionLocal, Broadcaster
OLD = 'https://old.example.com/webhook/twitch'   # 変更前のURL
with SessionLocal() as db:
    for b in db.query(Broadcaster).filter_by(platform='twitch'):
        for s in twitch_api.list_stream_online_subscriptions(b.channel_id):
            if s['transport'].get('callback') == OLD:
                twitch_api.delete_eventsub_subscription(s['id']); print('deleted', b.id)
"
```

## 8. DBの変更とロールバック

- `broadcasters.platform`（`youtube`/`twitch`、既定値 `youtube`）と `subscriptions.platform` を追加しました。
- 一意制約は `(platform, channel_id)` です。Twitchでは `channel_id` に `broadcaster_user_id` を入れるので、同じ文字列でもYouTubeとTwitchは別チャンネルになります。
- `subscriptions → broadcasters` の外部キーは `(platform, youtube_channel_id)` の複合キーです。列名 `youtube_channel_id` は旧コードとの互換のため変えていません（Twitch行ではTwitchのIDが入ります）。
- Twitch専用の列：`twitch_login`、`twitch_broadcaster_user_id`、`twitch_subscription_id`、`twitch_subscription_status`、`twitch_subscription_checked_at`、`twitch_subscription_error`。
- 新しいテーブル：`twitch_eventsub_messages`（受信済みメッセージID）。
- 2026-09-23追加（`migrations/2026-09-23_add_twitch_archive_links.sql`）：`broadcasters` に配信終了購読の列3つ（`twitch_offline_subscription_id` / `_status` / `_error`）と、テーブル `twitch_stream_notifications`（アーカイブ差し替え用の送信済みメッセージ記録）。取り消しは同名の `.rollback.sql`。

ロールバック（Twitch対応前のコードに戻して再起動してから実行）：

```bash
docker exec -i yt-haisin-bot-db-1 psql -U user -d youtube_bot_db -v ON_ERROR_STOP=1 \
    < migrations/2026-09-22_add_platform_twitch.rollback.sql
```

Twitchの行が残っているとロールバックは何も変更せずに停止します。消してよい場合の削除SQLはファイル冒頭に書いてあります。YouTubeのデータは削除しません。

## 9. テスト

```bash
docker run --rm -v "$PWD":/app -w /app -e PYTHONPATH=/app -e PYTHONDONTWRITEBYTECODE=1 \
    --entrypoint python yt-haisin-bot-web -m unittest discover -s tests -v
```

外部には通信せず、インメモリDBで実行します。`tests/test_twitch_integration.py` がTwitch機能とYouTubeの非回帰を、`tests/test_subscription_stability.py` が既存のYouTube購読処理を検証します。
