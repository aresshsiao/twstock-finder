from loguru import logger
import pandas as pd

_logger = logger.bind(module='Stock')

def combined_stocks(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """ 合併兩個 DataFrame，但只保留 'Code' 同時存在於兩者的股票 """
    # 確保 'Code' 欄位存在
    if "Code" not in df1.columns or "Code" not in df2.columns:
        raise ValueError("兩個 DataFrame 必須包含 'Code' 欄位")
    # 取出 df1 和 df2 都有的股票代碼
    common_codes = set(df1["Code"]) & set(df2["Code"])
    # 只保留 'Code' 在 common_codes 裡的資料
    df1_filtered = df1[df1["Code"].isin(common_codes)]
    df2_filtered = df2[df2["Code"].isin(common_codes)]
    # 依 'Code' 進行合併（inner join）
    df_combined = pd.merge(df1_filtered, df2_filtered, on=["Code", "Name"], how="inner")
    return df_combined
