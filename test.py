import os
import datetime
from loguru import logger
import stock
import pandas as pd
import yfinance as yf

LOGS_PATH = os.path.join(os.path.dirname(__file__), 'logs')
_logger = logger.bind(module='test')

def filter_explosive_stocks(df: pd.DataFrame, range_day=10, stable_rate=1.333, min_volume=500000) -> pd.DataFrame:
    """ 找出符合條件的股票：
        - 前 1天的50MA 成交均量 > min_volume 股（避免成交量過小的股票）
        - 當日成交量 > range_day MA 成交均量 * 2（爆大量）
        - 前 range_day 天的每日成交量都小於 range_day MA * 1.333
        - 當日收盤價 > 5MA、10MA、20MA（突破均線）
    """
    selected_stocks = []
    selected_dfs = []
    print(f"找出符合條件的股票 range_day={range_day} req_volume={min_volume} ...")

    def get_moving_averages(code: str):
        """ 從 Yahoo Finance 取得 5MA、10MA、20MA、10VolumeMA """
        try:
            ticker = f"{code}.TW"
            stock = yf.Ticker(ticker)
            df = stock.history(period="60d")  # 取最近 60 天數據（確保有 50 天數據可計算）

            if df.empty or len(df) < 60:
                print(f"無法獲取 {ticker} 的數據")
                return None, None

            df["5MA"] = df["Close"].rolling(window=5, min_periods=1).mean()
            df["10MA"] = df["Close"].rolling(window=10, min_periods=1).mean()
            df["20MA"] = df["Close"].rolling(window=20, min_periods=1).mean()
            df["50VolumeMA"] = df["Volume"].rolling(window=20, min_periods=1).mean()
            df[f"{range_day}VolumeMA"] = df["Volume"].rolling(window=10, min_periods=1).mean()

            return df.iloc[-1], df.iloc[-(range_day+1):-1]  # 取得最新一天 & 前 20 天數據

        except Exception as e:
            print(f"獲取 {code} 移動均線時發生錯誤: {e}")
            return None, None

    for i, row in df.iterrows():
        code = row["Code"]

        latest, prev_days = get_moving_averages(code)
        if latest is None or prev_days is None:
            continue

        # 判斷前 1 天的 50MA 成交均量是否大於 N 股
        if prev_days.iloc[-1]["50VolumeMA"] > min_volume:
            # 判斷前 N 天的成交量是否都小於 10MA * stable_rate
            if all(prev_days["Volume"] < prev_days[f"{range_day}VolumeMA"] * stable_rate):
                # 判斷當日成交量是否 > {range_day} VolumeMA * (stable_rate*2)
                if latest["Volume"] > latest[f"{range_day}VolumeMA"] * (stable_rate*2):
                # if latest["Volume"] > latest[f"{range_day}VolumeMA"]* (stable_rate*2) and latest["Close"] > latest["5MA"] and latest["Close"] > latest["10MA"] and latest["Close"] > latest["20MA"]:
                    selected_stocks.append(row)
                    selected_dfs.append(df)
                else:
                    print(f"{code} 不符合爆大量")
            else:
                print(f"{code} 可能爆過大量")
        else:
            print(f"{code} 成交量過小")

    return pd.DataFrame(selected_stocks), selected_dfs

if __name__ == '__main__':
    logger.add(os.path.join(LOGS_PATH, f'{datetime.date.today():%Y%m%d}.log'),
               rotation='1 day',
               retention='7 days',
               level='DEBUG')
    # 測試函數
    df_day = stock.twse_get_day()
    df_bwibbu = stock.twse_get_bwibbu()
    df_combined = stock.combined_stocks(df_day, df_bwibbu)
    print(df_combined)
    # **篩選掉成交金額低於 5000 萬的股票**
    df_filtered = df_combined[df_combined["TradeValue"] >= 40000000].reset_index(drop=True)
    # **篩選掉成交量低於 500 張的股票**
    df_filtered = df_filtered[df_filtered["TradeVolume"] >= 500000].reset_index(drop=True)
    # **篩選掉 PE > 30 的股票**
    # df_filtered = df_filtered[df_filtered["PBratio"] <= 2].reset_index(drop=True)
    # **篩選掉 PB > 7 的股票**
    # df_filtered = df_filtered[df_filtered["PBratio"] <= 2].reset_index(drop=True)
    # print(df_filtered)
    df_explosive, explosive_dfs = filter_explosive_stocks(df_filtered, range_day=10, min_volume=500000)
    print(df_explosive)
