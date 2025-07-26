import pandas as pd

def get_recent_df(df:pd.DataFrame, days=300) -> pd.DataFrame:
    """
    최근 n일의 데이터를 가져온다.
    기본값은 300일로 설정되어 있다.
    
    Args:
        df (pd.DataFrame): 원본 데이터프레임
            index는 '일자'와 '종목코드'로 멀티인덱스가 설정되어 있다. 
        days (int, optional): 최근 며칠의 데이터를 가져올지 지정. 기본값은 300일.
    Returns:
        pd.DataFrame: 최근 n일의 데이터프레임
    """
    # '일자'만 추출 (멀티인덱스의 첫 번째 레벨)
    dates = df.index.get_level_values('일자').unique()
    dates = pd.to_datetime(dates).sort_values()

    if len(dates) <= days:
        return df

    recent_dates = dates[-days:]
    return df.loc[df.index.get_level_values('일자').isin(recent_dates)]