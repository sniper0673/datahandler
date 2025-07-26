import pandas as pd
import requests
import time

# Fixme: 장 시잔 전에 불렀을 때 어떠한지 확인하기
def fetch_daily_stock_prices_from_nxt(date: pd.Timestamp) -> pd.DataFrame:
    """
    columns = ['일자', '종목코드', '표준코드', '종목명', '마켓구분', '종가', '전일대비', '변동률', 
        '시가', '고가', '저가', '거래량', '거래대금', '시장ID']
    
    넥스트레이드에서 지정한 날짜의 주식 데이터를 크롤링하여 DataFrame으로 반환.
    
    Parameters:
        date (str): 조회할 날짜 (예: "20250304") > YYYYMMDD 형식으로 입력. 
            참고로 넥스트레이드 최초 거래일은 20250304이다. 
    
    Returns:
        pd.DataFrame: 한글 컬럼명이 적용된 데이터프레임
    """
    date = pd.to_datetime(date).normalize()  # 날짜를 정규화하여 시간 부분 제거
    str_date = pd.to_datetime(date).strftime('%Y%m%d')  # 날짜 형식 변환
    
    columns_mapping = {
        'isuSrdCd': '종목코드',
        'isuCd': '표준코드',
        'isuAbwdNm': '종목명',
        'mktNm': '마켓구분',
        'curPrc': '종가',
        'contrastPrc': '전일대비',
        'upDownRate': '변동률',
        'oppr': '시가',
        'hgpr': '고가',
        'lwpr': '저가',
        'accTdQty': '거래량',
        'accTrval': '거래대금',
        'mktId': '시장ID',
    }
    
    url = "https://nextrade.co.kr/brdinfoTime/brdinfoTimeListAll.do"
    
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://nextrade.co.kr",
        "Referer": "https://nextrade.co.kr/menu/transactionStatusMain/menuList.do",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    
    payload = {
        "pageUnit": "20",
        "scAggDd": str_date,
        "scMktId": "",
        "searchKeyword": "",
        "_search": "false",
        "nd": str(int(time.time() * 1000)),
        "pageIndex": "1",
        "sidx": "",
        "sord": "asc"
    }

    response = requests.post(url, headers=headers, data=payload)

    if not response.ok:
        raise Exception(f"요청 실패: {response.status_code}")
    
    data = response.json().get("rows", [])
    
    if not data:
        # raise Exception("데이터 없음")
        return pd.DataFrame(columns=columns_mapping.values())  # 빈 데이터프레임 반환
    df = pd.DataFrame(data)
    df_selected = df[list(columns_mapping.keys())].rename(columns=columns_mapping)

    # 종목코드: 앞 'A' 제거하고 6자리로 보정
    df_selected['종목코드'] = df_selected['종목코드'].apply(
        lambda x: (str(x[1:]) if x.startswith('A') else str(x)).zfill(6)
    )
    df_selected['변동률'] = df_selected['변동률'] / 100  # 변동률을 백분율로 변환
    df_selected['일자'] = date  # 일자 컬럼 추가
    return df_selected[['일자'] + [col for col in df_selected.columns.tolist() if col != '일자']]

def test_fetch(date: str):
    """
    테스트용 함수: 지정한 날짜의 주식 데이터를 크롤링하여 엑셀 파일로 저장.
    
    Parameters:
        date (str): 조회할 날짜 (예: "20250502")
    """
    df = fetch_daily_stock_prices_from_nxt(date)
    df.to_excel(f"nextrade_data_{date}.xlsx", index=False)
    print(f"Finished: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    
if __name__ == "__main__":
    # 예시 날짜: 2025년 5월 2일
    test_date = "20250304"
    test_fetch(test_date)