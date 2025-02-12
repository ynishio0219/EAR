def calculate_ema(values, alpha):
    if alpha <= 0 or alpha > 1:
        raise ValueError("alpha must be between 0 and 1")

    ema = [0] * len(values)  # EMAの初期化

    if len(values) == 1:
        ema[0] = values[0]  # 初期値
    else:
        ema[0] = values[0]
        for t in range(1, len(values)):
            ema[t] = alpha * values[t] + (1 - alpha) * ema[t - 1]

    return ema
