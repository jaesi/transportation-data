"""
행수가 과다하게 많아져 희소한 OD 데이터(전체 행 대비 50%-75% 해당)를 '기타'로 집계

input: 일별/시간대별 집계 파일
output: 집계된 파일
"""


import duckdb
import argparse, os, csv, time, traceback, sys
from pathlib import Path
from datetime import datetime, date, timedelta
import duckdb

from utils import date_range


con = duckdb.connect()

# 1. 파일로드 
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--mem_gb", type=int, default=100)
    args=ap.parse_args()

    s = datetime.strptime(args.start, "%Y-%m-%d").date()
    e = datetime.strptime(args.end, "%Y-%m-%d").date()
    
    for d in date_range(s, e):
        dt = d.isoformat()
        dt_ = dt.replace('-','_')
        t0 = time.time()
              
        output_path = f'output/od_agg/final/{dt}/{dt}_agg_result_simplified.parquet'
        
        # 파일 있을 시 스킵
        if Path(output_path).exists():
            print(f"skipping: {dt} already exists;;...")
            continue
        
        try:
            print(f"...processing... {dt}")
            con.execute(f"""
                COPY (
                WITH first_agg AS(
                    SELECT *
                    FROM read_parquet('output/od_agg/final/{dt}/{dt_}_agg_result.parquet')
                )
                -- 통행량 1건인 것들 '기타'로 결합
                -- 1) 승차 -> 기타
                SELECT dt
                    , 승차정류장ID
                    , '기타' AS 하차정류장ID
                    , '승차' AS 승하차구분 -- 영어 표기 오류로 인해 수정
                    , hour
                    , 승차지역코드
                    , NULL AS 하차지역코드
                    , 승차교통수단구분
                    , NULL AS 하차교통수단구분
                    , SUM(통행량) AS 통행량
                    , SUM(통행명수) AS 통행명수
                    , AVG(평균이동거리) AS 평균이동거리
                    , MEDIAN(중위이동거리) AS 중위이동거리
                    , AVG(평균탑승시간) AS 평균탑승거리
                    , MEDIAN(중위탑승시간) AS 중위탑승시간
                    , AVG(평균환승건수) AS 평균환승건수
                    , MEDIAN(중위환승건수) AS 중위환승건수
                    , 승차정류장명칭
                    , 승차_gridid
                    , '기타' AS 하차정류장명칭
                    , NULL AS 하차_gridid
                FROM first_agg
                WHERE 통행량 = 1
                    AND 승하차구분 = 'alight'
                GROUP BY dt, 승차정류장ID, 승차정류장명칭, 승차지역코드, 승차교통수단구분, 승하차구분, hour, 승차_gridid
                
                -- 2) 하차 -> 기타
                UNION ALL
                SELECT dt
                    , '기타' AS 승차정류장ID
                    , 하차정류장ID
                    , '하차' AS 승하차구분 -- 영어 표기 오류로 인해 수정
                    , hour
                    , NULL AS 승차지역코드
                    , 하차지역코드
                    , NULL AS 승차교통수단구분
                    , 하차교통수단구분
                    , SUM(통행량) AS 통행량
                    , SUM(통행명수) AS 통행명수
                    , AVG(평균이동거리) AS 평균이동거리
                    , MEDIAN(중위이동거리) AS 중위이동거리
                    , AVG(평균탑승시간) AS 평균탑승거리
                    , MEDIAN(중위탑승시간) AS 중위탑승시간
                    , AVG(평균환승건수) AS 평균환승건수
                    , MEDIAN(중위환승건수) AS 중위환승건수
                    , '기타' AS 승차정류장명칭
                    , NULL AS 승차_gridid
                    , 하차정류장명칭
                    , 하차_gridid
                FROM first_agg
                WHERE 통행량 = 1
                    AND 승하차구분 = 'board'
                GROUP BY dt, 하차정류장ID, 하차정류장명칭, 하차지역코드, 하차교통수단구분, 승하차구분, hour, 하차_gridid
                -- 3) 통행량 1보다 큰 건들은 유지
                UNION ALL
                SELECT * 
                FROM first_agg
                WHERE 통행량 > 1
                ) TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            print(f"[SUC] {dt} processed! Elapsed time: {time.time()-t0:.1f}s")
        except Exception as e:
            # 작업 중이던 파일 삭제
            print(f"[FAILED] {dt} failed!: {e}")
            if os.path.exists(output_path):
                os.remove(output_path)
                print(f"...{dt} deleted.")

if __name__ == '__main__':
    main()