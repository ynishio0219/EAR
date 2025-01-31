# EMA関数 = 
def calculate_ema(series, alpha):
    if alpha <= 0 or alpha > 1:
        raise ValueError("alpha must be between 0 and 1")

    ema = [0] * len(series)  # EMAの初期化

    if len(series) == 1:
        ema[0] = series.iloc[0]  # 初期値
    else:
        ema[0] = series.iloc[0]
        for t in range(1, len(series)):
            ema[t] = alpha * series.iloc[t] + (1 - alpha) * ema[t - 1]

    return pd.Series(ema, index=series.index)  # インデックスを元データに合わせる
