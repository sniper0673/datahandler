import pandas as pd
from typing import List, Any

from sqlalchemy import text

# 커스텀 로깅 설정
from mystockutil.logging.logging_setup import CustomAdapter, logger as original_logger
logger = CustomAdapter(original_logger, {'prefix': 'SQLUtility'})

# ✅ DataFrame과 DB 테이블의 컬럼 목록을 비교해, 누락된 컬럼만 반환
def find_missing_columns(df: pd.DataFrame, table_columns: List[str]) -> List[str]:
    return [col for col in df.columns if col not in table_columns]

# ✅ pandas dtype을 SQL 타입 문자열로 매핑
def map_dtype_to_sql(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'  # 문자열 또는 기타 object 타입

# ✅ 누락된 컬럼들을 ALTER TABLE로 추가
def add_columns_to_table(connection, table_name: str, missing_cols: List[str], df: pd.DataFrame) -> None:
    for col in missing_cols:
        sql_type = map_dtype_to_sql(df[col].dtype)  # pandas dtype → SQL 타입
        alter_sql = f'ALTER TABLE {table_name} ADD COLUMN {col} {sql_type}'  # SQL 생성
        try:
            connection.execute(text(alter_sql))  # SQL 실행
            print(f"{col} 칼럼이 추가되었습니다.({sql_type}) to {table_name}.")
        except Exception as e:
            logger.error(f"칼럼 추가가 실패했습니다. {col} to {table_name}: {e}")
