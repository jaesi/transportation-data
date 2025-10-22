"""
반출한 GRID데이터를 5179좌표계로 변환하는 프로세스

Input: 원본 parquet
Output: 그리드컬럼을 제외한 5179
"""

import pandas as pd
from shapely.geometry import Point
import pyarrow, duckdb, argparse

X_MAPPING = list("ABCDEFG")
Y_MAPPING = list("BCDEFGH")

def _convert_single_grid_to_point(grid_id: str) -> Point | None:
    if not isinstance(grid_id, str) or len(grid_id) < 12:
        return None

    try:
        x_char, y_char = grid_id[0], grid_id[1]

        x_index = X_MAPPING.index(x_char)
        y_index = Y_MAPPING.index(y_char)

        x_tail = int(grid_id[2:7])
        y_tail = int(grid_id[7:])

    except (ValueError, IndexError):
        return None

    x_coord = (x_index + 7) * 100_000 + x_tail
    y_coord = (y_index + 14) * 100_000 + y_tail

    return Point(x_coord, y_coord)

def grid_to_5179_wkt(grid_ids:pd.Series) -> pd.Series:
    """
    DuckDB가 지리공간 타입을 직접 지원하지 않음으로
    그리드 ID 시리즈 -> EPSG:5179의 WKT 문자열 시리즈로 변환함.
    """
    if isinstance(grid_ids, (pyarrow.ChunkedArray, pyarrow.Array)):
        # to_pandas() 메서드를 사용하여 Series로 변환
        grid_ids = grid_ids.to_pandas()

    points = grid_ids.apply(_convert_single_grid_to_point)

    return points.apply(lambda p: p.wkt if p is not None else None)

"""파서를 받아서 grid 붙여서 그리드 컬럼 제외하고 parquet으로 저장해주는 것"""

def main():
    # Parser 설정
    parser = argparse.ArgumentParser(description="교통카드 데이터 후처리 파이프라인. Grid로 기록된 것을 5179 좌표로 전환함.")
    parser.add_argument("--input_path", type=str, required=True,
                        help="입력 parquet 파일 경로")
    parser.add_argument("--output_path", type=str, required=True,
                        help="출력 parquet 파일 경로")
    parser.add_argument("--grid_id_col", type=str, required=True,
                        help="좌표 변환에 사용할 grid ID 컬럼 이름 (승차/하차 접두사 제외, 예: '_gridid')")
    parser.add_argument("--new_coords_col", type=str, required=True,
                        help="새롭게 정의될 5179좌표 컬럼명 (승차/하차 접두사 제외, 예: '_coords_5179')")
    args = parser.parse_args()

    print("데이터 처리 시작...")

    # DuckDB 연결
    con = duckdb.connect()

    # UDF 등록
    con.create_function('GRID_TO_5179_UDF',
                        grid_to_5179_wkt,
                        ['VARCHAR'],
                        'VARCHAR',
                        type='arrow',
                        null_handling='special')

    last_cte_name = "source"
    ctes = [f"""{last_cte_name} AS (
                SELECT * 
                FROM read_parquet('{args.input_path}'))"""]
    cols_to_exclude = []

    # 승하차 + prefix 동시 적용
    for idx, prefix in enumerate(['승차', '하차'], start=1):
        step_num = idx
        new_cte_name = f"step{step_num}_add_{prefix}_coord"

        grid_col = f"{prefix}{args.grid_id_col}"
        coords_col = f"{prefix}{args.new_coords_col}"

        ctes.append(f"""
            {new_cte_name} AS (
                SELECT *, GRID_TO_5179_UDF("{grid_col}") AS "{coords_col}" 
                FROM {last_cte_name}
            )
        """)
        last_cte_name = new_cte_name
        cols_to_exclude.append(grid_col)

        with_clause = "WITH " + ", ".join(ctes)
        exclude_clause = f"EXCLUDE ({', '.join([f'\"{col}\"' for col in cols_to_exclude])})" if cols_to_exclude else ""

    # 최종 쿼리 조합
    query = f"""COPY ( 
                {with_clause} SELECT * 
                                    --{exclude_clause} -- 컬럼 제외 해제 
                    FROM {last_cte_name} ) 
                    TO '{args.output_path}' (FORMAT 'PARQUET');"""

    print("\n--- 실행될 최종 쿼리 ---\n", query, "\n--------------------")
    con.execute(query)
    con.close()

    print(f"Grid -> 5179 처리 완료! 결과가 '{args.output_path}'에 저장되었습니다.")

if __name__ == "__main__":
    main()