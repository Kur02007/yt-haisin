import requests

MAXRES_URL_TEMPLATE = "https://img.youtube.com/vi/{video_id}/maxresdefault.jpg"
HQDEFAULT_URL_TEMPLATE = "https://img.youtube.com/vi/{video_id}/hqdefault.jpg"


def get_best_thumbnail(video_id):
    """
    サムネイル画像が存在するか確認し、最適なURLを返す
    maxresdefault (最高画質) がなければ hqdefault (高画質) を使う
    """
    maxres_url = MAXRES_URL_TEMPLATE.format(video_id=video_id)

    try:
        # 画像のヘッダーだけ取得して存在チェック (ダウンロードはしないので高速)
        response = requests.head(maxres_url, timeout=2)
        if response.status_code == 200:
            return maxres_url
    except requests.exceptions.RequestException:
        pass

    # maxresがない、またはエラーの場合はhqを使う（hqはほぼ確実に存在する）
    return HQDEFAULT_URL_TEMPLATE.format(video_id=video_id)
