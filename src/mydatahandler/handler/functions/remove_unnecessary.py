import pandas as pd

def remove_unnecessary_symbols(df:pd.DataFrame) -> pd.DataFrame:
    """
    불필요한 종목 제거
    인덱스를 리셋 후, 다시 설정해 준다. 
    인덱스와 칼럼에 중복된 항목이 없어야 한다. 
    
    df - pd.DataFrame
        인덱스 또는 칼럼에 종목명, 종목코드가 반드시 포함되어 있어야 한다.
        
    """
    # 
    if set(df.index).intersection(df.columns):
        raise ValueError("DataFrame의 인덱스와 칼럼에 중복된 항목이 있습니다.")
    
    index_names = df.index.names
    df = df.reset_index(drop=False)
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
    df = df.set_index(index_names, drop=True)
    return df


if __name__ == '__main__':
    pass