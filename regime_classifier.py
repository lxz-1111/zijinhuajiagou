def _create_features(self, data):
    return pd.DataFrame({
        'sma10': data['close'].rolling(10).mean(),
        'rsi': 100 - (100 / (1 + data['close'].diff().clip(lower=0).mean() / data['close'].diff().clip(upper=0).abs().mean()))
    })