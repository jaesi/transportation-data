import zlib
import pandas as pd

def create_composite_pk_sql(columns: list[str], alias: str = "pk", separator: str = "_") -> str:
    """
    주어진 컬럼 리스트를 구분자로 이어붙이는 SQL 표현식 생성
    Args:
        columns (list[str]): 조합할 컬럼 이름 리스트
        alias (str): 생성될 PK 컬럼의 별칭 (alias)
        separator (str): 컬럼 값을 연결할 때 사용할 구분자

    Returns:
        str: DuckDB SQL 쿼리에서 사용할 수 있는 문자열
             예: "CAST("col1" AS VARCHAR) || '-' || CAST("col2" AS VARCHAR) AS pk"
    """

    if not columns:
        raise ValueError("PK를 생성할 컬럼이 최소 1개 이상 필요합니다.")

    # 각 컬럼을 문자열로 변환(CAST)하여 안전하게 결합
    concatenated_cols = f" || '{separator}' || ".join([f'CAST("{col}" AS VARCHAR)' for col in columns])

    return f'{concatenated_cols} AS "{alias}"'