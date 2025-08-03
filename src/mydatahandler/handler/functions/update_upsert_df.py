# Public imports
import pandas as pd

# Private imports

from mystockutil.logging.logging_setup import CustomAdapter, logger as original_logger
logger = CustomAdapter(original_logger, {'prefix': 'UpdateDFWithOtherDF'})

"""
DF를 다른 DF로 업데이트하는 모듈
"""
def update_df_with_another_df(
    df: pd.DataFrame, 
    another_df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    df에 있는 칼럼들의 값을 another_df의 값으로 업데이트한다. 
    동일한 index, 다른 columns 구조.
    
    Parameters:
    - df: pd.DataFrame
        원본 데이터프레임 (copy됨)
        index: 구조 동일
        columns: ['종가', '전일대비', '변동률', ...]
    - rdf_today: pd.DataFrame
        오늘 데이터프레임
        index: 구조 동일
        columns: 다양한 칼럼이 있을 수 있으며, df와 겹치는 칼럼만 업데이트

    Returns:
    - pd.DataFrame: 업데이트된 복사본
    """
    # 복사 및 소팅
    df = df.sort_index()
    another_df = another_df.sort_index()
    
    # 공통 칼럼 추출
    common_columns = set(df.columns).intersection(another_df.columns)
    # common_columns.discard('일자')
    # common_columns.discard('종목코드')

    if not common_columns:
        logger.warning("another_df와 df에 공통 칼럼이 없습니다. 업데이트하지 않습니다.")
        return df

    # 인덱스 교집합 추출
    valid_index = another_df.index.intersection(df.index)
    invalid_index = another_df.index.difference(df.index)

    if len(invalid_index) > 0:
        logger.warning(f"another_df의 일부 인덱스가 df에 존재하지 않습니다. 무시됨. count={len(invalid_index)}")

    if len(valid_index) == 0:
        logger.warning("another_df에 df의 index와 동일한 인덱스가 없어 업데이트하지 않습니다.")
        return df

    # 실제 업데이트
    df.loc[valid_index, list(common_columns)] = another_df.loc[valid_index, list(common_columns)]
    print(f"{len(valid_index)}개 인덱스에서 {len(common_columns)}개 칼럼 업데이트 완료.")
    df.sort_index(inplace=True)  # 인덱스 정렬
    return df

def upsert_df_with_similar_df(
    df: pd.DataFrame, 
    similar_df: pd.DataFrame, 
    ) -> pd.DataFrame:
    """
    df를 다른 동일한 구조의 similar_df로 업데이트 합니다. df가 비어있으면 similar_df를 그대로 반환합니다.
    """
    df = df.copy()  # 원본 DataFrame을 변경하지 않도록 복사본 생성
    if df.empty:
        logger.warning("df가 비어있습니다. similar_df를 그대로 반환합니다.")
        df = similar_df
    elif similar_df.empty:
        # similar_df가 비어있으면 df를 그대로 반환
        logger.warning("similar_df가 비어있습니다. df를 그대로 반환합니다.")
    else:
        # similar_df의 인덱스(종목코드)와 df의 인덱스가 겹치는 부분을 찾습니다.
        common_indices = df.index.intersection(similar_df.index)
        # 겹치는 부분을 df에서 제거합니다 (업데이트를 위해)
        df_without_common = df.drop(common_indices)
        # 제거된 DataFrame에 새로운 stock_info를 concat합니다.
        df = pd.concat([df_without_common, similar_df])
    
    # 필요에 따라 인덱스 정렬
    return df.sort_index()