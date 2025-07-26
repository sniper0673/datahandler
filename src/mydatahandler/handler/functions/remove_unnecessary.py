import pandas as pd

def remove_unnecessary_symbols(df:pd.DataFrame) -> pd.DataFrame:
    """
    불필요한 종목 제거
    df - pd.DataFrame
        - index: 없는 것으로 가정한다.
        - column: 종목코드와 종목명이 포함되어 있어야 함
    """
    # 
    df = df.copy()
    df = df.reset_index(drop=True)
    df = df[
    ~df['종목명'].str.contains(r'\d+호$') & 
    ~df['종목명'].str.contains('스팩') &
    df['종목코드'].str.endswith('0')
    ]
    if '시장ID' in df.columns:
        # 코스피, 코스닥 시장만 남김
        df = df[
        df['시장ID'].str.contains('STK|KSQ')
        ]
    return df


if __name__ == '__main__':
    pass