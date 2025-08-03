from typing import List
import pandas as pd
from functools import wraps

from mystockutil.logging.logging_setup import CustomAdapter, logger as original_logger
logger = CustomAdapter(original_logger, {'prefix': 'SDH'})

"""
주식 데이터를 다루는 기본 클래스
"""

from mydatahandler.handler.functions import remove_unnecessary_symbols, get_recent_df, update_df_with_another_df, upsert_df_with_similar_df
from mydatahandler.handler.singleday_data_handler import SingledayDataHandler

class _StockDataHandler:
    def __init__(self, df:pd.DataFrame=None):
        self.date_col_name = '일자'
        self.symbol_col_name = '종목코드'
        self.primary_keys = [self.date_col_name, self.symbol_col_name]
        self.sdh:SingledayDataHandler = SingledayDataHandler()
        self._df:pd.DataFrame = pd.DataFrame(
            columns=['일자', '종목코드']
            ).set_index(pd.MultiIndex.from_tuples([], names=['일자', '종목코드']))  # 초기화 시 빈 DataFrame으로 설정
        if df is not None:
            self.set_data(df)  # df가 None이 아닐 경우, copy하여 저장
    
    @property
    def df(self) -> pd.DataFrame:
        """
        self._df를 반환합니다.
        index = ['일자', '종목코드']
        columns = ['종가', '전일대비', '변동률', '시가', '고가', '저가', '거래량', '거래대금', ...] 등의 여러가지가 있을 수 있음
        """
        return self._df
    @df.setter
    def df(self, value: pd.DataFrame):
        """
        self._df를 value로 설정합니다.
        """
        if not isinstance(value, pd.DataFrame):
            raise ValueError("value must be a pandas DataFrame.")
        self._df = self._set_data(value)
    def set_data(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        df로 데이터를 설정합니다. 
        자동으로 인덱스를 설정합니다. 
        """
        self.df = df # self._set_data() 호출
        return df
    def _set_data(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        서버나 feather에서 가져온 df를 index를 설정하고 정렬하여 초기화 후, 
        self.df에 저장한다.
        """
        df = df.copy()  # 원본 DataFrame을 변경하지 않도록 복사본 생성
        df = self._convert_index_to_primary_keys(df)
        df = df.sort_index()  # 인덱스 정렬
        try:
            last_date = df.index.get_level_values(self.date_col_name).unique()[-1]  
            # SingledayDataHandler에 오늘 데이터 설정
            self.sdh.set_data(df=df.loc[last_date,:])
        except IndexError:
            last_date = None
        return df
    
    def _convert_index_to_primary_keys(self, df:pd.DataFrame, drop:bool=True) -> pd.DataFrame:
        df = df.copy()  # 원본 DataFrame을 변경하지 않도록 복사본 생성
        # 현재 인덱스를 리셋
        if set(self.primary_keys) == set(df.index.names):
            index_in_cols = set(df.columns).intersection(set(df.index.names))
            df = df[[col for col in df.columns if col not in index_in_cols]] # 인덱스와 동일한 칼럼을 제외한 나머지 칼럼만 선택
            df.reset_index(inplace=True, drop=False)
        else:
            if df.index.names == [None] or df.index.names is None:
                df.reset_index(inplace=True, drop=True)
            else:
                # 단순한 인덱스가 아닌 경우(즉, 인덱스의 이름이 None이 아닌 경우)에는 인덱스를 칼럼으로 보존
                index_in_cols = set(df.columns).intersection(set(df.index.names))
                df = df[[col for col in df.columns if col not in index_in_cols]] # 인덱스와 동일한 칼럼을 제외한 나머지 칼럼만 선택
                df.reset_index(inplace=True, drop=False)
        # 필요한 인덱스가 모두 있는지 확인. 
        if not set(self.primary_keys).issubset(set(df.columns)):
            raise ValueError(f"DataFrame must contain columns: {self.primary_keys}")
        # 인덱스를 primary_keys로 설정
        df[self.date_col_name] = pd.to_datetime(df[self.date_col_name]).dt.normalize() # narmalize()를 사용하기 위해서 .dt 사용
        df.set_index(self.primary_keys, inplace=True, drop=drop)
        return df
    
    def _sort_df(self):
        # df를 정렬한다. 일자, 종목코드 순으로 정렬한다.
        if self.df is not None:
            self.df.sort_index(level=['일자', '종목코드'], inplace=True, ascending=[True, True])
        else:
            logger.error("DataFrame is not set.")
            # raise ValueError("DataFrame is not set.")


class StockDataHandler_sdh(_StockDataHandler):
    """
    SingledayDataHandler를 이용한 메쏘드
    """
    def get_stock_name(self, symbol:str) -> str:
        """
        종목코드에 해당하는 종목명을 반환합니다.
        """
        return self.sdh.get_stock_name(stock_symbol=symbol)
    def get_stock_names(self, symbols:list) -> list:
        """
        종목코드 리스트에 해당하는 종목명을 반환합니다.
        """
        return self.sdh.get_stock_names(stock_symbols=symbols)
    def get_stock_symbol(self, stock_name:str) -> str:
        """
        종목명에 해당하는 종목코드를 반환합니다.
        """
        return self.sdh.get_stock_symbol(stock_name=stock_name)
    def get_stock_symbols(self, stock_names:list) -> list:
        """
        종목명 리스트에 해당하는 종목코드를 반환합니다.
        """
        return self.sdh.get_stock_symbols(stock_names=stock_names)


class StockDataHandler_property(StockDataHandler_sdh):
    def sdf(self, symbol:str)->pd.DataFrame:
        return self.df.xs(key=symbol, level='종목코드')
    def by_symbol(self, symbol:str)->pd.DataFrame:
        return self.df.xs(key=symbol, level='종목코드', drop_level=False)
    def today_by_symbol(self, symbol:str)->pd.DataFrame:
        """
        오늘 날짜에 해당하는 종목의 데이터를 반환합니다.
        """
        idx = pd.IndexSlice
        return self.df.loc[idx[pd.Timestamp.today().normalize(), symbol], :]

    def by_symbols(self, symbols:List[str])->pd.DataFrame:
        idx = pd.IndexSlice
        return self.df.loc[idx[:, pd.Index(symbols)], :]
    def today_by_symbols(self, symbols:List[str])->pd.DataFrame:
        """
        오늘 날짜에 해당하는 종목들의 데이터를 반환합니다.
        """
        idx = pd.IndexSlice
        return self.df.loc[idx[pd.Timestamp.today().normalize(), pd.Index(symbols)], :]

    @property
    def symbols(self)->list:
        """
        전 일자에 걸친 모든 종목 코드 리스트를 반환합니다.
        """
        return self.df.index.get_level_values('종목코드').unique().tolist()
    @property
    def today_symbols(self)->list:
        """
        데이터의 제일 마지막 날짜에 해당하는 일자의 종목 코드 리스트를 반환한다.
        """
        return self.df_today.index.get_level_values('종목코드').unique().tolist()
    @property
    def date_list(self)->list:
        return self.df.index.get_level_values('일자').unique().tolist()
    @property
    def last_date(self)->pd.Timestamp:
        return self.df.index.get_level_values('일자').unique()[-1]
    @property
    def tdf(self)->pd.DataFrame: # 오늘일자의 df, 일자 칼럼은 삭제 / 
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-1])
    @property
    def ydf(self)->pd.DataFrame: # 어제일자의 df, 일자 칼럼은 삭제
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-2])
    def pdf(self, n:int)-> pd.DataFrame: # n일전의 df, 일자 칼럼은 삭제
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-n])
    def date_df(self, date:pd.Timestamp)->pd.DataFrame: # 특정일자의 df, 일자 칼럼은 삭제
        date = pd.to_datetime(date).normalize()  # 날짜를 정규화
        if date not in self.date_list:
            raise ValueError(f"날짜 {date}에 해당하는 데이터가 없습니다.")
        return self.df.xs(key=date, level='일자')
    @property
    def df_filtered(self): # filtered df
        df = remove_unnecessary_symbols(self.df)
        df.set_index(self.primary_keys, inplace=True, drop=False)
        return df
    def remove_unnecessary_symbols(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        불필요한 종목 코드를 제거합니다.
        """
        df = remove_unnecessary_symbols(df)
        return df
        
    @property
    def df_today(self): # 오늘일자의 df, 일자 칼럼 포함
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-1], drop_level=False)
    @property
    def df_yesterday(self):
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-2], drop_level=False)
    def df_previous(self, n):
        return self.df.xs(self.df.index.get_level_values('일자').unique()[-n], drop_level=False)
    def by_date(self, date:pd.Timestamp):
        date = pd.to_datetime(date).normalize()  # 날짜를 정규화
        return self.df.xs(key=date, level='일자', drop_level=False)
    def df_after(self, date:pd.Timestamp, include_date:bool=False) -> pd.DataFrame:
        """
        특정 날짜 이후의 데이터를 반환한다.
        date는 pd.Timestamp 형식이어야 한다.
        """
        date = pd.to_datetime(date).normalize()
        if include_date:
            return self.df.loc[self.df.index.get_level_values('일자') >= date, :]
        else:
            return self.df.loc[self.df.index.get_level_values('일자') > date, :]
    def df_before(self, date:pd.Timestamp, include_date:bool=False) -> pd.DataFrame:
        """
        특정 날짜 이전의 데이터를 반환한다.
        date는 pd.Timestamp 형식이어야 한다.
        """
        date = pd.to_datetime(date).normalize()
        if include_date:
            return self.df.loc[self.df.index.get_level_values('일자') <= date, :]
        else:
            return self.df.loc[self.df.index.get_level_values('일자') < date, :]
    def df_from_to(self, from_date:pd.Timestamp, to_date:pd.Timestamp) -> pd.DataFrame:
        """
        특정 날짜 범위의 데이터를 반환한다.
        from_date와 to_date는 pd.Timestamp 형식이어야 한다.
        """
        from_date = pd.to_datetime(from_date).normalize()
        to_date = pd.to_datetime(to_date).normalize()
        return self.df.loc[(self.df.index.get_level_values('일자') >= from_date) & 
                            (self.df.index.get_level_values('일자') <= to_date), :]


class _StockDataHandler_manage(StockDataHandler_property):
    def _change_data_type_with_mapping(self, dtype_mapping:dict):
        """ df의 type을 변경 """
        
        # DataFrame에 존재하는 열만 필터링하여 astype() 적용
        existing_columns = {col: dtype for col, dtype in dtype_mapping.items() if col in self.df.columns}

        # astype() 적용
        print(f"Changing data type of df...")
        self.set_data(df=self.df.astype(existing_columns))


class _StockDataHandler_recent(_StockDataHandler_manage):
    def get_recent_df(self, days:int=300) -> pd.DataFrame:
        """
        최근 n일의 데이터를 가져온다.
        기본값은 300일로 설정되어 있다.
        """
        if self.df.empty:
            logger.warning("DataFrame is empty. Returning an empty DataFrame.")
            raise ValueError("DataFrame is empty. Please set data first.")
        # 최근 n일의 데이터를 가져온다.
        recent_df = get_recent_df(df=self.df, days=days)
        return recent_df
    def set_as_recent_df(self, days:int=300):
        """
        현재 df를 최근 n일의 데이터로 설정한다.
        기본값은 300일로 설정되어 있다.
        """
        if self.df.empty:
            logger.warning("DataFrame is empty. Cannot set as recent DataFrame.")
            raise ValueError("DataFrame is empty. Please set data first.")
        self.set_data(get_recent_df(df=self.df, days=days))
        
class _StockDataHandler_add_del(_StockDataHandler_recent):
    def add_single_daily_df(self, daily_df:pd.DataFrame):
        """
        특정일의 주식 데이터를 추가한다.
        현재 아무런 데이터도 없는 경우에도 작동한다. 
        인덱스가 중복되는 경우, 기존 데이터를 덮어쓰고 추가한다.
        daily_df는 [일자, 종목코드]를 인덱스로 가지는 멀티인덱스 DataFrame이거나, 
        칼럼에 '일자'와 '종목코드'가 있는 DataFrame이어야 한다.
        """
        # 현재 아무런 데이터도 없는 경우.
        if self.df.empty:
            # 하루짜리 날자를 가지는 DataFrame으로 설정한다. 
            self.set_data(daily_df)
        # self.sdh.df가 비어있지 않은 경우.
        else:
            # 기존 데이터와 중복되는 날짜가 있다면, 해당 날짜의 데이터를 제거하고 추가한다.
            # daily_df의 인덱스를 설정
            # daily_df = self._convert_index_to_date_symbol(daily_df)
            daily_df = self._convert_index_to_primary_keys(daily_df)
            overlap_idx = self.df.index.intersection(daily_df.index)
            self.df = pd.concat([self.df.drop(overlap_idx), daily_df])
        self._sort_df()

    def del_date(self, date:pd.Timestamp):
        """
        특정 날짜의 데이터를 삭제한다.
        date는 pd.Timestamp 형식이어야 한다.
        """
        date = pd.to_datetime(date).normalize()
        if date not in self.date_list:
            logger.warning(f"날짜 {date}에 해당하는 데이터가 없습니다. 삭제하지 않습니다.")
            return
        df = self.df[self.df.index.get_level_values('일자') != date]
        self.set_data(df)
        print(f"날짜 {date}에 해당하는 데이터를 삭제했습니다.")
        
    # def _convert_index_to_date_symbol(self, daily_df:pd.DataFrame, drop:bool=True) -> pd.DataFrame:
    #     """
    #     DataFrame의 인덱스를 self.primary_keys로 설정하고, 정렬한다.
    #     """
    #     if set(self.primary_keys) != set(daily_df.index.names):
    #         # 우선 인덱스를 없앤다. 
    #         if daily_df.index.names == [None]:
    #             daily_df.reset_index(drop=True, inplace=True)
    #         else:
    #             daily_df.reset_index(drop=False, inplace=True)
    #         # self.primary_keys가 모두 daily_df의 칼럼에라도 있는지 확인
    #         if not set(self.primary_keys).issubset(set(daily_df.columns)):
    #             raise ValueError(f"daily_df의 인덱스가 {self.primary_keys}와 일치하지 않습니다.")
    #         # self.primary_keys를 인덱스로 설정한다.
    #         daily_df.set_index(self.primary_keys, drop=drop, inplace=True)
    #         daily_df.sort_index(inplace=True, ascending=[True, True])
    #     return daily_df


class StockDataHandler_update_sert(_StockDataHandler_add_del):
    """
    오늘(제일 마지막일)의 데이터를 다루는 핸들러 클래스
    """
    def update_df_with_another_df(self, another_df:pd.DataFrame, save:bool=True) -> pd.DataFrame:
        """
        self.df에 있는 칼럼들의 값을 rdf_today의 값으로 수정한 후, 리턴한다.
        Params:
        another_df:pd.DataFrame
            index: ['일자', '종목코드']
            columns: 여러가지 칼럼이 모두 가능. self.df와 겹치는 칼럼에 대해서만 업데이트
        save:bool
            True이면 self.df에 저장하고, False이면 저장하지 않는다.
        참고:
        self.df:
            index: ['일자', '종목코드']
            columns: ['종가', '전일대비', '변동률', '시가', '고가', '저가', '거래량', '거래대금', ...] 등의 여러가지가 있을 수 있음
        """
        # another_df의 인덱스를 self.primary_keys로 설정하고, 정렬한다.
        another_df = self._convert_index_to_primary_keys(another_df)
        df = update_df_with_another_df(
            df=self.df, 
            another_df=another_df
        )
        if save:
            self.set_data(df)
        return df
    
    def upsert_df_with_similar_df(self, similar_df:pd.DataFrame, save:bool=True) -> pd.DataFrame:
        """
        self.df에 another_df의 값을 업데이트한다. 
        another_df가 비어있으면 self.df를 그대로 반환한다.
        Params:
            similar_df:pd.DataFrame
                index 또는 칼럼에 '일자', '종목코드'가 있어야 한다.
            save:bool
                True이면 self.df에 저장하고, False이면 저장하지 않는다.
        """
        if similar_df.empty:
            logger.warning("another_df가 비어있습니다. self.df를 그대로 반환합니다.")
            return self.df
        # similar_df의 인덱스를 self.primary_keys로 설정하고, 정렬한다.
        similar_df = self._convert_index_to_primary_keys(similar_df)
        df = upsert_df_with_similar_df(
            df=self.df, 
            similar_df=similar_df
        )
        if save:
            self.set_data(df)
        return df

class StockDataHandler(StockDataHandler_update_sert):
    """
    생성시 df 패러메터를 주면서 호출하거나, 
    set_data 메서드를 통해서 df를 설정할 수 있다.
    df는 [일자, 종목코드]를 인덱스로 가지는 멀티인덱스여야 한다.
    sdf, tdf 등의 xdf의 경우 멀티 인덱스를 제거하고 싱글인덱스
    df_xxx의 경우에는 멀티인덱스를 그대로 유지
    today(금일)의 기준은 df의 인덱스의 마지막 날짜이다. 
    """
    """df= pd.DataFrame, primary_keys=['일자', '종목코드']"""
    def __init__(self, df:pd.DataFrame=None):
        super().__init__(df=df)
    def ready(self):
        """
        초기화하지 않아도 됨.
        self.sdh.ready() > KRX의 최신 데이터를 로드합니다.
        """
        self.sdh.ready()
    def clear(self):
        """
        현재 데이터를 비웁니다.
        """
        self.set_data(pd.DataFrame(columns=self.df.columns).set_index(pd.MultiIndex.from_tuples([], names=self.primary_keys)))
        self.sdh.clear()
        print("Data cleared.")

if __name__ == "__main__":
    dh = StockDataHandler()
    dh.ready()
    print(dh.get_stock_name("005930"))  # 삼성전자
    
    print("Finished.")