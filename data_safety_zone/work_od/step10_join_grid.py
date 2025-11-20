"""
input: 월텍스트
output: 집계파일 루트에 그리드가 조인된 결과물
"""
import argparse, time, os
from pathlib import Path
import numpy as np
import pandas as pd

from config import CONFIG
from utils import db_connection, between_months

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args=ap.parse_args()

    start = int(args.start)
    end = int(args.end)

    months = between_months(start, end)
    
    con = db_connection()
    
    # 1. 월별 데이터 루프
    for month in months:
        output_path = f'output/purpose_transport/work_od_filtered/{month}/{month}_workod_purpose_transport_grid.parquet'
        # 경로 생성
        os.makedirs(Path(output_path).parent, exist_ok=True)

        # 파일 존재 유무 -> 스킵
        # if Path(output_path).exists():
        #     print(f"{month} final workod file exists already...;;")
        #     continue

        print(f"Processing {month} workod file...")
        # 조인 후 parquet 파일로 생성
        con.execute(f"""
            COPY (
            -- 1.월별통근OD 데이터에 조인
            WITH station_cleansed AS(
                SELECT *
                FROM read_parquet('output/station_cleansed/{month}/{month}15_station_cleansed.parquet')
                )
            SELECT p.승차정류장ID
                , s1.정류장명칭 AS 승차정류장명칭
                , p.승차지역코드
                , p.승차교통수단구분
                , s1.grid_id AS 승차그리드ID
                , p.하차정류장ID
                , s2.정류장명칭 AS 하차정류장명칭
                , p.하차지역코드
                , p.하차교통수단구분
                , s2.grid_id AS 하차그리드ID
                , p.trip_type AS 출퇴근구분
                , p.월_총_통행량
                , p.이동거리_평균
                , p.이동거리_중위
                , p.탑승시간_평균
                , p.탑승시간_중위
            FROM read_parquet('output/purpose_transport/work_od_filtered/{month}/{month}_workod_purpose_transport.parquet') p
            -- 2. 정제 정류장 데이터 조인
            -- 2.1. 승차 정보
            LEFT JOIN  station_cleansed s1
                ON p.승차정류장ID = s1.정류장ID
                AND p.승차지역코드 = s1.지역코드
                AND p.승차교통수단구분 = s1.교통수단구분
            -- 2.2. 하차 정보
            LEFT JOIN  station_cleansed s2
                ON p.하차정류장ID = s2.정류장ID
                AND p.하차지역코드 = s2.지역코드
                AND p.하차교통수단구분 = s2.교통수단구분
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """).df()
        print(f"    [SUC] finally {month} workod file done!")

if __name__ == '__main__':
    main()