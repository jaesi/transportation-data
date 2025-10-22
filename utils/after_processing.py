import duckdb
import argparse, json

from pk import create_composite_pk_sql
from coordinates import grid_to_5179_wkt


# 1. 메인 처리 로직
def main():
    # Parser 설정
    parser = argparse.ArgumentParser(description="교통카드 데이터 후처리 파이프라인. 기본적으로 모든 작업을 수행하며, 특정 작업만 수행하려면 플래그를 사용하세요.")
    # parser.add_argument('--pk', action='store_true', help='복합 PK 생성')
    parser.add_argument('--coords', action='store_true', help='좌표 변환 작업')

    parser.add_argument("--input_path", type=str, required=True,
                        help="입력 parquet 파일 경로")
    parser.add_argument("--output_path", type=str, required=True,
                        help="출력 parquet 파일 경로")
    parser.add_argument("--composite_key_cols", type=str, required=True,
                        help="PK 생성에 사용할 컬럼 이름들을 쉼표로 구분 (승차/하차 접두사 제외, 예: '정류장ID,지역코드,교통수단구분')")
    parser.add_argument("--common_key_cols", type=str, default="",
                        help="PK 생성에 사용할 공통 컬럼들 ex. 승하차구분")
    parser.add_argument("--grid_id_col", type=str, required=True,
                        help="좌표 변환에 사용할 grid ID 컬럼 이름 (승차/하차 접두사 제외, 예: '_gridid')")
    parser.add_argument("--new_pk_col", type=str, required=True,
                        help="새롭게 정의될 PK 컬럼명 (승차/하차 접두사 제외, 예: '_pk')")
    parser.add_argument("--new_coords_col", type=str, required=True,
                        help="새롭게 정의될 5179좌표 컬럼명 (승차/하차 접두사 제외, 예: '_coords_5179')")
    args = parser.parse_args()

    # 공통 컬럼명들 (접두사 제외)
    base_key_cols_list = [col.strip() for col in args.composite_key_cols.split(',')]
    common_key_cols_list = [col.strip() for col in args.common_key_cols.split(',')] if args.common_key_cols else []

    # 좌표 변환과 해시 태스크 수행 여부 설정
    is_task_specified = args.pk or args.coords

    run_pk_task = args.pk or not is_task_specified
    run_coords_task = args.coords or not is_task_specified

    print("데이터 처리 시작...")

    # DuckDB 연결
    con = duckdb.connect()

    # 동적 쿼리 빌딩
    ctes = [f"source AS (SELECT * FROM read_parquet('{args.input_path}'))"]
    last_cte_name = "source"
    cols_to_exclude = []

    if run_pk_task:
        print(" - 복합 PK 생성 작업 추가 (승차/하차)")

        # 승차와 하차 각각 PK 생성
        for idx, prefix in enumerate(['승차', '하차'], start=1):
            new_cte_name = f"step{idx}_add_{prefix}_pk"

            # 접두사 붙는 컬럼들
            prefixed_key_cols = [f"{prefix}{col}" for col in base_key_cols_list]
            # 공통 컬럼들 추가
            all_key_cols = prefixed_key_cols + common_key_cols_list
            pk_col_name = f"{prefix}{args.new_pk_col}"

            pk_sql_expr = create_composite_pk_sql(all_key_cols, pk_col_name)
            ctes.append(f"{new_cte_name} AS (SELECT *, {pk_sql_expr} FROM {last_cte_name})")
            last_cte_name = new_cte_name
            # 주의: 공통 컬럼은 exclude하면 안 됨 (두 번 사용되므로)
            cols_to_exclude.extend(prefixed_key_cols)

    if run_coords_task:
        print(" - 좌표 변환 작업 추가 (승차/하차)")

        # UDF 한 번만 등록
        con.create_function('GRID_TO_5179_UDF',
                            grid_to_5179_wkt,
                            ['VARCHAR'],
                            'VARCHAR',
                            type='arrow',
                            null_handling='special')

        # 승차와 하차 모두 처리
        for idx, prefix in enumerate(['승차', '하차'], start=1):
            # step 번호 계산 (PK 작업 후에 이어서)
            step_num = (2 if run_pk_task else 0) + idx
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

    query = f"""COPY ( 
            {with_clause} SELECT * 
                                --{exclude_clause} -- 컬럼 제외 해제 
                        FROM {last_cte_name} ) 
                TO '{args.output_path}' (FORMAT 'PARQUET');"""

    print("\n--- 실행될 최종 쿼리 ---\n", query, "\n--------------------")
    con.execute(query)
    con.close()

    print(f"처리 완료! 결과가 '{args.output_path}'에 저장되었습니다.")


if __name__ == "__main__":
    main()