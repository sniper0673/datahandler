import pandas as pd
from typing import List

"""
하루짜리 데이터를 처리하는 핸들러입니다.
.df: pd.DataFrame
    index='종목코드'
    columns = ['일자', ... ]
"""

# essential imports
from mydatahandler.handler.functions import fetch_recent_usable_stock_prices_from_krx

class _SDH:
    """
    .df: pd.DataFrame
        index='종목코드'
        columns = ['일자', ... ]
    .date: pd.Timestamp # 데이터의 유일한(동일한) 날짜
    """
    def __init__(self):
        self._df = pd.DataFrame()  # 종목코드는 index에만 존재
        self.date: pd.Timestamp = None # self.df의 유일한(동일한) 날짜. 존재하지 않을 수도 있다. 

    @property
    def df(self) -> pd.DataFrame:
        """
        self._df를 반환합니다.
        index = '종목코드'
        columns = ['일자', ... ]
        """
        return self._df
    @df.setter
    def df(self, value: pd.DataFrame):
        """
        self._df를 value로 설정합니다.
        """
        self._df = self._set_data(value)
    
    def set_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        df를 self.df로 설정합니다.
        이 경우, 자동으로 index가 '종목코드'로 설정되고, '일자' 칼럼이 존재하는 경우, 이를 TimeStamp로 변환합니다.
        """
        self.df = df
        return df
    
    def _set_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        .df를 df로 설정합니다. 
        Params:
        df: pd.DataFrame
            index='종목코드' # 종목코드가 index가 아닌 경우, 인덱스를 리셋하고 '종목코드'를 인덱스로 만듭니다.
            columns = ['일자', ... ]
        """
        df = df.copy()
        # 인덱스 name과 동일한 칼럼을 제거
        if df.index.name in df.columns:
            df = df.drop(columns=[df.index.name])
        # '종목코드'를 index로 설정하기
        drop = False if df.index.name else True
        df.reset_index(drop=drop, inplace=True)
        if '종목코드' not in df.columns:
            raise ValueError("DataFrame must contain '종목코드' column.")
        # 중복된 칼럼을 삭제
        df = df.loc[:, ~df.columns.duplicated()]
        # '종목코드'를 index로 설정하면서 칼럼에서 삭제
        df.set_index('종목코드', drop=True, inplace=True)
        # 일자 칼럼이 존재하는 경우, TimeStamp로 변환 후, 모든 일자칼럼이 동일한지 확인. 
        if '일자' in df.columns:
            df['일자'] = pd.to_datetime(df['일자'], errors='coerce')
            if df['일자'].isnull().any():
                raise ValueError("Some '일자' values could not be converted to datetime.")
            if df['일자'].nunique() > 1:
                raise ValueError("All '일자' values must be the same.")
        return df.sort_index()
    
    def load_renect_data_from_krx(self):
        # pykrx를 통해서 금일 기본 정보를 받아온다.
        self.df = fetch_recent_usable_stock_prices_from_krx()
        print(f"One Day Data Handler: Loaded data with {len(self.df)} stocks.")


class SDH_get(_SDH):
    @property
    def stock_symbols(self) -> List[str]:
        """
        종목코드 리스트를 반환합니다.
        """
        return self.df.index.tolist()
    @property
    def stock_names(self) -> List[str]:
        """
        종목명 리스트를 반환합니다.
        """
        return self.df['종목명'].tolist()
    def get_stock_name(self, stock_symbol: str) -> str:
        try:
            return self.df.loc[[stock_symbol], '종목명'].iloc[0]
        except KeyError:
            raise ValueError(f"Stock symbol {stock_symbol} not found in the database.")
    
    def get_stock_names(self, stock_symbols: List[str]) -> List[str]:
        return [self.get_stock_name(symbol) for symbol in stock_symbols]
    def get_stock_symbol(self, stock_name: str) -> str:
        """
        종목명을 통해 종목코드를 반환합니다.
        """
        try:
            return self.df[self.df['종목명'] == stock_name].index[0]
        except IndexError:
            raise ValueError(f"Stock name {stock_name} not found in the database.")
    def get_stock_symbols(self, stock_names: List[str]) -> List[str]:
        """
        종목명 리스트를 통해 종목코드 리스트를 반환합니다.
        """
        return [self.get_stock_symbol(name) for name in stock_names]


class SingledayDataHandler(SDH_get):
    """
    하루짜리 데이터를 처리하는 핸들러입니다.
    .df: pd.DataFrame
        index='종목코드'
        columns = ['일자', ... ]
    """
    def ready(self):
        """
        데이터를 KRX의 최신 데이터로 준비합니다. 
        """
        self.load_renect_data_from_krx()
        print("Singleday Data Handler is ready with the latest KRX data.")
    def clear(self):
        """
        데이터를 초기화합니다. 
        즉, KRX의 최신 데이터를 다시 불러옵니다.
        """
        print("Clearing Singleday Data Handler...")
        self.df = pd.DataFrame(columns=self.df.columns, index=pd.Index([], name='종목코드'))


if __name__ == '__main__':
    sdh = SingledayDataHandler()  # data_provider는 실제로 사용하지 않음
    sdh.ready() # KRX의 최신 데이터를 로드합니다.
    print(sdh.get_stock_name('005930'))  # 삼성전자
    print("Finished testing One Day Data Handler")