import pandas as pd
import time

from pykrx.website.krx.market.core import 전종목시세

from mystockutil.logging.logging_setup import CustomAdapter, logger as original_logger
logger = CustomAdapter(logger=original_logger, extra={'prefix': 'WebKRX'})

def fetch_recent_usable_stock_prices_from_krx(market:str='ALL') -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    while True:
        df = fetch_daily_usable_stock_prices_from_krx(today, market)
        if not df.empty:
            break
        today -= pd.Timedelta(days=1)
        time.sleep(1)
    return df

def fetch_daily_usable_stock_prices_from_krx(date: pd.Timestamp=None, market:str='ALL') -> pd.DataFrame:
    """
    Returns: pd.DataFrame
        columns = [
            '일자', '종목코드', '표준코드', '종목명', '마켓구분', '관리구분', '종가', '변동코드', '전일대비', '변동률', 
            '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '시장ID', '기준가'
            ]
    휴일 또는 날짜 오류, 개장 전의 경우 빈 데이터프레임을 반환한다.
    """
    date = pd.Timestamp.today().normalize() if date is None else pd.Timestamp(date).normalize()
    market2mktid = {
        "ALL": "ALL",
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX"
    }
    df = 전종목시세().fetch(date.strftime('%Y%m%d'), market2mktid[market])
    col_new_names = [
        '종목코드', '표준코드', '종목명', '마켓구분', '관리구분', '종가', '변동코드', '전일대비', '변동률', 
        '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '시장ID'
    ]
    # 칼럼 이름을 한글로 변경
    df.columns = col_new_names

    # NaN 값을 처리: 숫자형 칼럼은 0 또는 -1로, 문자열 칼럼은 빈 문자열로 채우기
    df = df.fillna({'종목코드': '', '표준코드': '', '종목명': '', '마켓구분': '', '섹터구분': '', 
                    '종가': '-1', '변동코드': '-1', '전일대비': '0', '변동률': '0', 
                    '시가': '-1', '고가': '-1', '저가': '-1', '거래량': '-1', '거래대금': '-1', 
                    '시가총액': '-1', '상장주식수': '-1', '시장ID': ''})
    
    # 모든 칼럼에서 빈칸과 쉼표 제거
    df = df.apply(lambda x: x.str.replace('[, ]', '', regex=True) if x.dtype == "object" else x)
    # df에서 '-' 문자를 제거한다. 
    # 단, 전일대비와 변동률은 음수의 값을 가질 수 있다.
    col_to_apply = [col for col in df.columns if not col in ['전일대비', '변동률']]
    df[col_to_apply] = df[col_to_apply].apply(lambda x: x.str.replace(r'[\-]', '', regex=True) if x.dtype == "object" else x)
    
    """
    1. usable한 데이터가 아닌 경우.
    """
    if (df['전일대비']=='-').all():
        
        return pd.DataFrame(columns=['일자']+col_new_names)
    
    """
    # 2. usable한 데이터인 경우. 
    """
    # 가격에 해당하는 칼럼들을 int64로 변환
    price_columns = ['종가', '전일대비', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수']
    try:
        for col in price_columns:
            df[col] = df[col].astype('int64')
        df['기준가'] = df['종가'] - df['전일대비']
    except ValueError as e:
        # logger.error(f"Error: {e} while converting price columns to int64") 
        raise Exception(f"Error: {e} while converting price columns to int64")
        # 변환 중 오류가 발생(개장하지 않는 날)하면 빈 DataFrame 반환
        # return pd.DataFrame(columns=col_new_names)
    
    # 변동률 칼럼을 float로 변환하고 100으로 나누기
    df['변동률'] = df['변동률'].astype('float64') / 100
    # 일자는 pd.Timestamp로 통일시키기
    df['일자'] = pd.to_datetime(date).normalize()
    # df['일자'] = date.strftime('%Y-%m-%d')
    res_df = df[['일자']+[col for col in df.columns if col != '일자']]
    
    return res_df

if __name__ == '__main__':
    from mystockutil.df.format import myprint as print
    df = fetch_recent_usable_stock_prices_from_krx()
    
    print(f"Finished")