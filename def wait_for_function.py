def wait_for_file(file_path):
    import time
    import os

    # 監視するパス for test
    # file_path = '/Volumes/qpr/qpr_data_extract/rawdata/a6e3a73b-4345-49f9-a9b2-19c6f9a44561.csv'

    # 待機時間 (秒) - 10秒ごとにチェック
    wait_interval = 10

    # 最大待機時間 (秒) - 20分 (1200秒)まで待機
    max_wait_time = 3600

    # 経過時間のカウント
    elapsed_time = 0

    print(f"⏳ '{file_path}' の到着を待機中...")

    # ファイルが見つかるまでループ
    while not os.path.exists(file_path):
        time.sleep(wait_interval)
        elapsed_time += wait_interval
        print(f"⏳ {elapsed_time}秒経過... ファイルはまだ存在しません。")

        # 最大待機時間を超えた場合
        if elapsed_time >= max_wait_time:
            raise TimeoutError(
                f"⛔️ 指定したファイル '{file_path}' は {max_wait_time} 秒以内に見つかりませんでした。"
            )

    print(f"✅ ファイルが見つかりました！パス: {file_path}")
