import numpy as np
import pandas as pd 
import time, os, argparse
import duckdb
from glob import glob
from pathlib import Path 

# 목적통행 디렉토리 생성
output_base='output/purpose_transport'
os.makedirs(output_base, exist_ok=True)

## 목적통행 집계 
## 월별 순회하며 목적통행으로 집계하여 parquet으로 저장하는 프로세스

months = ['202501', '202502', '202503', '202504', '202505', '202506']

def is_duckdb_interrupt(e:Exception) -> bool:
    name = e.__class__.__name__.lower()
    msg= (str(e) or "").lower()
    return ("interrupt" in name or "interrupted" in msg)

def main(months):
    # DuckDB setting 
    con = duckdb.connect()
    con.execute(f"SET memory_limit='50GB'")

    # 월 순회
    for month in months:
        
        days = glob(f"import_data/TB_KTS_DWTCD_METROPOLITAN/{month}/**.csv")
        length_df= []
        # output 폴더 만들기
        
        for day in days: 
            try: 
                t0 = time.time()
                print(f"Processing {day[-12:-4]} file for month={month}")
                output_base_path = f'output/purpose_transport/{month}/{day[-12:-4]}'
                os.makedirs(output_base_path, exist_ok=True)
                output_path = os.path.join(output_base_path, 'daily_purpose.parquet')
                
                # 이미 파일 존재할 경우 패스
                if Path(output_path).exists() and ~(Path(output_path/'_ERROR').exists()):
                    continue
                
                # 목적통행 집계 수행 
                con.execute(f""" 
                COPY(
                SELECT 운행일자
                    , 가상카드번호
                    , 트랜잭션ID
                    -- 첫 승차/ 마지막 하차
                    , TRY_CAST(arg_min(정산사승차정류장ID, 승차일시) AS BIGINT) AS 승차정류장ID
                    , TRY_CAST(arg_max(정산사하차정류장ID, 하차일시) AS BIGINT) AS 하차정류장ID
                    , MIN(승차일시) AS 승차일시
                    , MAX(하차일시) AS 하차일시
                    , arg_min(정산지역코드, 승차일시) AS 승차지역코드
                    , arg_max(정산지역코드, 하차일시) AS 하차지역코드
                    , CASE WHEN arg_min(교통수단코드, 승차일시) BETWEEN 200 AND 299 THEN 'T'
                        ELSE 'B' END AS 승차교통수단구분
                    , CASE WHEN arg_max(교통수단코드, 하차일시) BETWEEN 200 AND 299 THEN 'T'
                        ELSE 'B' END AS 하차교통수단구분
                    -- 집계
                    , SUM(이용거리) AS 총이동거리
                    , SUM(탑승시간) AS 총탑승시간
                    , MAX(환승건수) AS 최대환승건수
                FROM read_csv('{day}', 
                    types={{'정산사노선ID':'VARCHAR', '가상카드번호':'VARCHAR'}})
                WHERE "이용자유형코드(시스템)" = 1
                GROUP BY 운행일자, 가상카드번호, 트랜잭션ID
                HAVING MIN(승차일시) IS NOT NULL AND MAX(하차일시) IS NOT NULL
                )
                TO '{output_path}'
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 512000)
                ;
                """)

                length = con.execute(f"select count(*) from read_parquet('{output_path}');").fetchone()[0]
                # 실제 집계 수행
                print(f"    [SUC] Day {day[-8:4]}:{length} purpose trips, elapsed_time: {round(time.time()-t0, 1)}s")

                # 길이 메타 데이터 df로 저장
                length_df.append([{'day': day, 'length': length}])
                
            except KeyboardInterrupt:
                print("--stopping cleanly", flush=True)
                raise
            except Exception as e:
                if is_duckdb_interrupt(e):
                    raise KeyboardInterrupt from e 
                ## output path에 error 칩 추가
                Path(output_path/'_ERROR').touch()
                print(f"    [ERR] processing {day}: {e}")
    
        # 월별 결과 저장
        
        # 월별집계 결과 없을 경우 패스
        if Path('output'/'purpose_transport'/f'{month}'/f'{month}_purpose_transport.parqeut').exists():
            continue
        # length_df 쌓이지 않은 경우 패스
        if length_df != []:
            pd.concat([length_df]).to_csv(f'output/purpose_transport/{month}/{month}_length.csv')
            
        con.execute(f"""
        COPY(
            SELECT * FROM read_parquet('output/purpose_transport/{month}/*/*.parquet')
            )
        TO 'output/purpose_transport/{month}/{month}_purpose_transport.parquet'
        (FORMAT PARQUET, COMPRESSION zstd)
        """)
        
        print(f"[SUC] Processed and saved data for {month}")
        
if __name__ == '__main__':
    main(months)