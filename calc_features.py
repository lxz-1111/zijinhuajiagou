import pandas as pd

class YourModel:
    def _create_features(self, data):
        sma10 = data['close'].rolling(window=10, min_periods=1).mean()
        # ...（完整 RSI 计算逻辑）...
        return pd.DataFrame({'sma10': sma10, 'rsi': rsi})
