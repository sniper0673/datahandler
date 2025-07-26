from typing import List, Dict, Any, Tuple, Optional
import time
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re

def convert_market_cap(value: str) -> int:
    """
    시가총액 문자열을 정수형 숫자로 변환합니다.
    예: "348조 6,353억" -> 34863530000000
    """
    # 공백, 탭, 개행 문자 제거
    value = value.replace('\t', '').replace(' ', '').replace('\n', '')
    
    # 숫자만 추출
    trillion = re.search(r'(\d+)조', value)
    billion = re.search(r'(\d+(?:,\d+)*)억', value)
    
    trillion_val = int(trillion.group(1)) if trillion else 0
    billion_val = int(billion.group(1).replace(',', '')) if billion else 0
    
    return trillion_val * 10**12 + billion_val * 10**8

def convert_number(value: str) -> int:
    """
    쉼표 제거 후 정수 변환.
    예: "5,969,782,550" -> 5969782550
    """
    return int(value.replace(',', ''))

# 네이버 금융에서 주식 종목 정보를 가져오는 함수    
def fetch_acc_stock_info_from_naver_as_dict(stock_symbol)->Optional[Dict]:
    """
    columns = [
        '종목코드', '종목명', 
        '전일가', '현재가', '전일대비', 
        '변동률', 
        '기준가', '시가', '고가', '저가', 
        '종가', 
        '상한가', '하한가', 
        '거래량', '거래량_krx', '거래량_nxt', 
        '거래대금', '거래대금_krx', '거래대금_nxt', 
        '시가총액', '상장주식수',
        ]
    """
    res_columns = [
        '종목코드', '종목명', 
        '전일가', '현재가', '전일대비', 
        '변동률', 
        '기준가', '시가', '고가', '저가', 
        '종가', 
        '상한가', '하한가', 
        '거래량', '거래량_krx', '거래량_nxt', 
        '거래대금', '거래대금_krx', '거래대금_nxt', 
        '시가총액', '상장주식수',
        ]
    
    url = f"https://finance.naver.com/item/main.nhn?code={stock_symbol}"
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve data")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')

    tr_tags = soup.select_one('#tab_con1').find_all('tr')
    # tr 태그들을 순회하며 th와 td 추출
    tr_data = {}
    for tr in tr_tags:
        th = tr.find('th')
        td = tr.find('td')
        # th와 td가 존재할 때만 처리
        if th and td:
            # 항목명과 값 추출 및 정리
            item_name = th.get_text(strip=True).replace('\n', ' ')
            item_value = td.get_text(separator=' ', strip=True).replace('\n', ' ')
            # 딕셔너리에 저장
            tr_data[item_name] = item_value
    tr_data['시가총액'] = convert_market_cap(tr_data.get('시가총액', ''))
    tr_data['상장주식수'] = convert_number(tr_data.get('상장주식수', ''))
    # # 결과 출력
    # for key, value in tr_data.items():
    #     print(f"{key}: {value}")

    dl_tag = soup.select_one('#middle.new_totalinfo > dl')

    def get_value_from_dd_tags(key)->Dict[str, Any]:
        if dl_tag:
            # Extract the text from each <dd> tag
            dd_tags = dl_tag.find_all('dd')
            
            # Create a dictionary to store the data
            data_dict = {}
            
            for dd in dd_tags:
                text = dd.get_text()
                if key in text:
                    # Extract the value for key
                    value = text.split(' ')[1].replace(',', '').replace('백만', '000000')
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                    # print(f"{key} (Opening Price): {value}")
                    return value
                    break
            else:
                print("{key} (Opening Price) not found.")
        else:
            print("Element not found")
        return None
    
    stock_info = {}
    stock_info['종목코드'] = stock_symbol
    stock_info['종목명'] = soup.select_one("div.wrap_company > h2 > a").text.strip()
    stock_info['현재가'] = get_value_from_dd_tags('현재가')
    stock_info['전일가'] = get_value_from_dd_tags('전일가')
    stock_info['기준가'] = stock_info['전일가']
    stock_info['전일대비'] = int(stock_info['현재가']) - int(stock_info['전일가'])
    stock_info['변동률'] = float(stock_info['전일대비']) / float(stock_info['전일가'])
    stock_info['시가'] = get_value_from_dd_tags('시가')
    stock_info['종가'] = stock_info['현재가']
    stock_info['고가'] = get_value_from_dd_tags('고가')
    stock_info['저가'] = get_value_from_dd_tags('저가') 
    stock_info['상한가'] = get_value_from_dd_tags('상한가')
    stock_info['하한가'] = get_value_from_dd_tags('상한가')
    stock_info['거래량_krx'] = get_value_from_dd_tags('거래량')
    stock_info['거래대금_krx'] = get_value_from_dd_tags('거래대금')
    # 시가총액과 상장주식수는 위에서 이미 처리함
    stock_info['시가총액'] = tr_data['시가총액']
    stock_info['상장주식수'] = tr_data['상장주식수']
    
    #넥스트레이드 info 추출
    next_trade_info = soup.select_one('div#rate_info_nxt')
    deal_value = 0
    deal_volume = 0
    # ✅ 거래대금 추출 / 넥스트레이드 종목이 아닌 경우 실행하지 않음
    if next_trade_info:
        table = next_trade_info.find('table', class_='no_info')
        rows = table.find_all('tr')
        for row in rows:
            text = row.get_text()
            if '거래대금' in text:
                em = row.find_all('td')[-1].find('em')  # 오른쪽 끝 td의 em
                if em:
                    deal_value = em.find('span', class_='blind').text.strip().replace(',', '')
            elif '거래량' in text:
                em = row.find_all('td')[-1].find('em')  # 오른쪽 끝 td의 em
                if em:
                    deal_volume = em.find('span', class_='blind').text.strip().replace(',', '')
    else:           
        pass
        # print('❗ rate_info_nxt 영역이 없음')
    stock_info['거래대금_nxt'] = int(deal_value) * 1e6 # 백만단위
    stock_info['거래량_nxt'] = int(deal_volume)
    stock_info['거래량'] = stock_info['거래량_krx'] + stock_info['거래량_nxt']
    stock_info['거래대금'] = stock_info['거래대금_krx'] + stock_info['거래대금_nxt']
    return {col:stock_info[col] for col in res_columns if col in stock_info}  # 필요한 칼럼만 반환

def fetch_nxt_trading_value_from_naver(symbol:str)->int:
    """
    종목코드에 해당하는 종목의 넥스트레이드 거래대금을 가져오는 함수
    """
    info = fetch_acc_stock_info_from_naver_as_dict(symbol)
    return info['거래대금_nxt']

def fetch_trading_value_from_naver(symbol:str)->int:
    """
    종목코드에 해당하는 종목의 거래대금을 가져오는 함수
    """
    info = fetch_acc_stock_info_from_naver_as_dict(symbol)
    return info['거래대금']

def get_intraday_chart_from_naver(stock_code, minute=1)->pd.DataFrame:
    """ 
    Index(['종가', '시가', '고가', '저가', '거래량', '평균가', '추정거래대금', '누적거래대금'], dtype='object')
    """
    if minute not in [1, 3, 5, 10, 30, 60]:
        print("Invalid minute value")
        return None
    if minute == 1:
        minute = "" # 1분봉 데이터는 minute 파라미터를 사용하지 않음
    url = f"https://api.stock.naver.com/chart/domestic/item/{stock_code}/minute{minute}"
    response = requests.get(url)
    if response.status_code != 200:
        print("Failed to retrieve data")
        return pd.DataFrame()
    # DataFrame으로 변환 후, 시간을 datetime 형식으로 변환 후 index로 설정
    df = pd.DataFrame(response.json())
    if df.empty:
        return df
    # localDateTime을 datetime 형식으로 변환
    df['localDateTime'] = pd.to_datetime(df['localDateTime'], format='%Y%m%d%H%M%S')
    df.columns = ['datetime', '종가', '시가', '고가', '저가', '거래량']
    df['평균가'] = df[['종가', '시가', '고가', '저가']].mean(axis=1)
    df['추정거래대금'] = df['평균가'] * df['거래량']
    df['누적거래대금'] = df['추정거래대금'].cumsum()
    df.set_index('datetime', inplace=True)
    return df

# symbols 리스트를 일정 크기씩 쪼개주는 함수
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_multiple_current_ohlcv_from_naver(symbols:List[str])->pd.DataFrame:
    """
    columns = [
        '종목코드', '종목명', '정규장', '넥스트', 
        '전일가', '현재가', '전일대비', 
        '변동률', '변동률_nxt', '변동률_장후', 
        '기준가', '시가', '고가', '저가', 
        '종가', '종가_krx', '종가_nxt', 
        '상한가', '하한가', 
        '거래량', '거래대금',
        'KEPS', 'EPS', 'BPS', 'cnsEPS', 'DV'
        ]
    """
    res_columns = [
        '종목코드', '종목명', '정규장', '넥스트', 
        '전일가', '현재가', '전일대비', 
        '변동률', '변동률_nxt', '변동률_장후', 
        '기준가', '시가', '고가', '저가', 
        '종가', '종가_krx', '종가_nxt', 
        '상한가', '하한가', 
        '거래량', '거래대금', 
        'KEPS', 'EPS', 'BPS', 'cnsEPS', 'DV'
        ]
    all_data = []  # 결과를 저장할 리스트

    # symbols 리스트를 chunk_size만큼 쪼개서 요청
    chunk_size = 1000
    for symbol_chunk in chunks(symbols, chunk_size):
        # Fixme: 급히 추가함
        symbol_chunk = [s for s in symbol_chunk if s]  # 숫자만 필터링
        url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_RECENT_ITEM:{','.join(symbol_chunk)}"
        response = requests.get(url)

        if response.status_code != 200 or response.json().get('resultCode') != 'success':
            print("Failed to retrieve data for symbols:", symbol_chunk)
            continue

        # 응답 데이터 파싱 및 DataFrame으로 변환
        res = response.json()
        df = pd.DataFrame(res['result']['areas'][0]['datas'])

        if df.empty:
            continue
        # Fixme: sv가 무슨 값인지는 multiple changed 종목이 있는 경우 확인할 수 있을 듯
        column_mapping = {
            'cd': '종목코드', 
            'nm': '종목명', 
            'sv': '기준가', 
            'nv': '종가', 
            'cv': '전일대비', 
            'cr': '변동률', 
            'rf': 'rf', 
            'mt': 'mt', 
            'ms': '정규장', 
            'tyn': 'tyn', 
            'pcv': '전일가', 
            'ov': '시가', 
            'hv': '고가', 
            'lv': '저가', 
            'ul': '상한가', 
            'll': '하한가', 
            'aq': '거래량', 
            'aa': '거래대금', 
            'nav': 'nav', # 시총 같은나 실제로 값을 가지지 않음
            'keps': 'KEPS', 
            'eps': 'EPS', 
            'bps': 'BPS', 
            'cnsEps': 'cnsEPS', 
            'dv': 'DV',
            'nxtOverMarketPriceInfo': "NXT"
            }
        df.rename(columns=column_mapping, inplace=True)
        
        # 넥스트레이드 거래정보 처리
        df_nxt = df['NXT'].apply(pd.Series)
        df = pd.concat([df, df_nxt], axis=1)
        nxt_column_mapping = {
            "tradingSessionType": "거래세션", # REGULAR_MARKET, ...(확인필요)
            "overMarketStatus": "넥스트", # OPEN, CLOSE, ...(확인필요)
            "overPrice": "종가_nxt",
            "compareToPreviousPrice": "이전가대비", 
            "compareToPreviousClosePrice": "전일대비_nxt",
            "fluctuationsRatio": "변동률_nxt",
            "localTradedAt": "거래시각",
            "tradeStopType": "거래정지정보", # 장운영 상태 등에 관한 Dict
        }
        df.rename(columns=nxt_column_mapping, inplace=True)
        df.drop(columns=['NXT'], inplace=True, errors='ignore')
        # 칼럼수정
        fv_nxt = df['종가_nxt'].fillna('').str.replace(',', '')
        df['종가_nxt'] = pd.to_numeric(fv_nxt, errors='coerce', downcast='integer')
        
        df['정규장'] = np.where(df['정규장'] == 'CLOSE', '폐장', '개장')
        df['넥스트'] = np.where(df['넥스트'] == 'CLOSE', 
                            '폐장', 
                            np.where(df['넥스트'] == 'OPEN', 
                                    '개장', 
                                    '제외'
                                    )
                            )
        # 전일대비 및 변동률 계산
        df['종가_krx'] = df['종가']  # KRX 종가
        df['시가_krx'] = df['시가']  # KRX 시가
        df['고가_krx'] = df['고가']  # KRX 고가
        df['저가_krx'] = df['저가']  # KRX 저가
        df['전일대비'] = df['종가_krx'] - df['기준가']
        
        # 넥스트레이드가 없는 경우, krx와 동일한 값을 갖도록 설정해준다. 혹시모를 에러 방지
        df['종가_nxt'] = np.where(df['넥스트'] == '제외',
                                df['종가_krx'], 
                                df['종가_nxt']
                                )
        
        df['변동률'] = df['전일대비'] / df['기준가']
        df['변동률_nxt'] = np.where(df['넥스트'] == '제외',
                                df['변동률'], 
                                df['변동률_nxt']
                                )
        df['변동률_장후'] = np.where(df['넥스트'] == '제외', 
                                0,
                                df['종가_nxt'] / df['종가_krx'] - 1
                                )
        # 종가 결정
        df['종가'] = np.where((df['정규장']=='폐장') & (df['넥스트']=='개장'), 
                            df['종가_nxt'],
                            df['종가_krx']
                            )  # 종가는 정규장 상태에 따라 결정
        
        # 통일성을 위해 칼럼추가
        df['현재가'] = df['종가']  # 현재가는 종가와 동일
        
        all_data.append(df)  # 결과를 리스트에 추가
        time.sleep(1)

    if not all_data:
        return pd.DataFrame()  # 만약 데이터가 없다면 빈 DataFrame 반환

    # 여러 DataFrame을 하나로 합침
    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df[res_columns]
    return final_df

def test_multiple_current_ohlcv_from_naver():
    import pandas as pd
    # Test the function with a list of stock symbols
    symbols = ['005930', '000660', '035420', '005380', '051910', '066620']
    # symbols = ['298380']
    df = get_multiple_current_ohlcv_from_naver(symbols)
    print(df.head())
def test_fetch_stock_info_from_naver_as_dict():
    stock_symbol = '005930'  # 삼성전자
    stock_info = fetch_acc_stock_info_from_naver_as_dict(stock_symbol)
    if stock_info:
        print("Stock Info:")
        for key, value in stock_info.items():
            print(f"{key}: {value}")
    else:
        print("Failed to fetch stock info")

def test_multiple_trading_value_from_naver():
    symbols = ['005930', '000660', '035420', '005380', '051910']
    for symbol in symbols:
        trading_value = fetch_trading_value_from_naver(symbol)
        time.sleep(1)
        print(f"Trading value for {symbol}: {trading_value:,}")
    
if __name__ == '__main__':
    from mystockutil.df.format import myprint as print
    # test_multiple_current_ohlcv_from_naver()
    # test_fetch_stock_info_from_naver_as_dict()
    # test_multiple_trading_value_from_naver()
    # test_multiple_current_ohlcv_from_naver()
    df = get_multiple_current_ohlcv_from_naver(['005930'])
    print("Finished")