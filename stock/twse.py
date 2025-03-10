from loguru import logger
import requests
import pandas as pd

_logger = logger.bind(module='Stock')

def twse_get_bwibbu(code: str = ''):
    """ 取得台灣證交所的本益比 (PE)、股利率 (Dividend Yield)、股價淨值比 (PB) 數據 """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
    try:
        response = requests.get(url)
        if response.status_code < 200 and response.status_code >= 300:
            _logger.error(f"{url} status={response.status_code}")
            return None
        data = response.json()
        if not isinstance(data, list):
            print("無法獲取數據")
            return None
        # columns = ["股票代碼", "股票名稱", "本益比", "股利率", "股價淨值比"]
        stocks = pd.DataFrame(data)
        # **定義需要轉型的欄位**
        float_cols = ["PEratio", "DividendYield", "PBratio"]        
        # **數值轉換函數，處理空字串與 NaN**
        def safe_convert(value, dtype):
            try:
                return dtype(value) if value != "" else None  # 遇到空字串轉換為 None
            except ValueError:
                return None  # 若轉換失敗則設為 None
        # **應用轉換**
        for col in float_cols:
            stocks[col] = stocks[col].apply(lambda x: safe_convert(x.replace(",", ""), float))  # 移除逗號並轉型
        # 若有指定股票代碼，則回傳該股票資料
        if code:
            stock_data = stocks.loc[stocks["Code"] == code]
            return stock_data.to_dict(orient="records")[0] if not stock_data.empty else None
        else:
            return stocks
    except Exception as e:
        print(f"獲取本益比數據時發生錯誤: {e}")
        return None

def twse_get_day(code = ''):
    """ 取得台灣證交所當日所有股票交易數據，或查詢特定股票 """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    try:
        response = requests.get(url)
        if response.status_code < 200 and response.status_code >= 300:
            _logger.error(f"{url} status={response.status_code}")
            return None
        data = response.json()
        if not isinstance(data, list):
            print("無法獲取數據")
            return None
        # columns = ["股票代碼", "股票名稱", "成交量", "成交價", "開盤價", "最高價", "最低價", "收盤價"]
        stocks = pd.DataFrame(data)
        # **定義需要轉型的欄位**
        int_cols = ["TradeVolume", "TradeValue"]
        float_cols = ["OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice"]        
        # **數值轉換函數，處理空字串與 NaN**
        def safe_convert(value, dtype):
            try:
                return dtype(value) if value != "" else None  # 遇到空字串轉換為 None
            except ValueError:
                return None  # 若轉換失敗則設為 None
        # **應用轉換**
        for col in int_cols:
            stocks[col] = stocks[col].apply(lambda x: safe_convert(x.replace(",", ""), int))  # 移除逗號並轉型
        for col in float_cols:
            stocks[col] = stocks[col].apply(lambda x: safe_convert(x.replace(",", ""), float))  # 移除逗號並轉型
        # 若有指定股票代碼，則回傳該股票資料
        if code:
            stock_data = stocks.loc[stocks["Code"] == code]
            return stock_data.to_dict(orient="records")[0] if not stock_data.empty else None
        else:
            return stocks

    except Exception as e:
        _logger.error(f"獲取台股數據時發生錯誤: {e}")
        return None

if __name__ == '__main__':

    # 測試獲取所有台股數據
    bwibbu_data = twse_get_bwibbu()
    print(bwibbu_data)
    all_stocks = twse_get_day()
    print(all_stocks)

    # 測試查詢特定股票（例如台積電 2330）
    tsmc = twse_get_day("2330")
    print(tsmc)