import os
import time
import datetime
from loguru import logger
import stock
import pandas as pd
import yfinance as yf

LOGS_PATH = os.path.join(os.path.dirname(__file__), 'logs')
_logger = logger.bind(module='test')

def get_moving_averages(code: str, range_day):
    """ 從 Yahoo Finance 取得 5MA、10MA、20MA、10VolumeMA """
    try:
        ticker = f"{code}.TW"
        stock = yf.Ticker(ticker)
        total_day = range_day+22
        df = stock.history(period=f"{total_day}d")  # 取最近 33 天數據（確保有 33 天數據可計算）
        if df.empty or len(df) < total_day:
            print(f"無法獲取 {ticker} {total_day}天的數據")
            return None, None, None
        df["5MA"] = df["Close"].rolling(window=5, min_periods=1).mean()
        df["10MA"] = df["Close"].rolling(window=10, min_periods=1).mean()
        df["20MA"] = df["Close"].rolling(window=20, min_periods=1).mean()
        df["5VolumeMA"] = df["Volume"].rolling(window=5, min_periods=1).mean()
        df["10VolumeMA"] = df["Volume"].rolling(window=10, min_periods=1).mean()
        df["20VolumeMA"] = df["Volume"].rolling(window=20, min_periods=1).mean()
        return df.iloc[-1], df.iloc[-2], df.iloc[-(range_day):-2]  # 取得今天, 昨天, 前天, 跟之前天數據

    except Exception as e:
        if str(e) == 'Too Many Requests. Rate limited. Try after a while.':
            time.sleep(60)
        print(f"獲取 {code} 移動均線時發生錯誤: {e}")
        return None, None, None

def is_explosive_stock(code, range_day=10, stable_rate=4/3, min_volume=500000):
    today, prev_day1, prev_days = get_moving_averages(code, range_day)
    total_day = range_day+22
    if today is None or prev_day1 is None is None or prev_days is None:
        print(f"{code} 數據有問題")
        return False
    # 20MA成交均量是否大於 N 股
    if prev_day1[f"{range_day}VolumeMA"] < min_volume:
        print(f"{code} 成交量過小")
        return False
    # 判斷是否連兩日紅K
    if today["Close"] <= prev_day1["Open"] or prev_day1["Close"] <= prev_days.iloc[-1]["Open"]:
        print(f"{code} 沒有連兩日紅K")
        return False
    # 判斷是否連兩日往上
    if today["Close"] <= prev_day1["Close"] or prev_day1["Close"] <= prev_days.iloc[-1]["Close"] or \
        today["Low"] <= prev_day1["Low"] or prev_day1["Low"] <= prev_days.iloc[-1]["Low"] or \
        prev_day1["High"] <= prev_days.iloc[-1]["High"]:
        print(f"{code} 沒有連兩日往上")
        return False
    # 判斷前 N 天的成交量是否都小於 {range_day}VolumeMA * stable_rate
    if not all(prev_days["Volume"] < prev_days[f"{range_day}VolumeMA"] * stable_rate):
        print(f"{code} {total_day}日內已經有過大量")
        return False
    # 判斷是否連兩日量增
    if today["Volume"] <= prev_day1["Volume"] or prev_day1["Volume"] <= prev_days.iloc[-1]["Volume"]:
        print(f"{code} 沒有連兩日量增")
        return False
    # 判斷當日成交量是否大於 {range_day}VolumeMA * (stable_rate*3/2)
    if today["Volume"] < today[f"{range_day}VolumeMA"] * (stable_rate*3/2):
        print(f"{code} 沒有暴大量")
        return False
    # 當日收盤價 > 5MA、10MA、20MA（突破均線）
    if today["Close"] < today["5MA"] or today["Close"] < today["10MA"]:
        print(f"{code} 沒有站上5日/10日均線")
        return None
    # if today["Close"] < today["20MA"]:
    #     print(f"{code} 沒有站上月均線")
    #     return None
    
    print(f"{code} 符合條件")
    return True

def filter_explosive_stocks(df: pd.DataFrame, range_day=10, stable_rate=5/4, min_volume=500000) -> pd.DataFrame:
    """ 找出符合條件的股票：
        - 前 1天的20MA 成交均量 > min_volume 股（避免成交量過小的股票）
        - 當日成交量 > range_day MA 成交均量 * 2（爆大量）
        - 前 range_day 天的每日成交量都小於 range_day MA * 1.333
    """
    selected_stocks = []
    selected_dfs = []
    print(f"找出符合條件的股票 range_day={range_day} req_volume={min_volume} ...")

    for i, row in df.iterrows():
        code = row["Code"]
        if is_explosive_stock(code, range_day):
            selected_stocks.append(row)
            selected_dfs.append(df)

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
