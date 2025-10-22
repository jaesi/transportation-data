import argparse, os, csv, time, traceback, sys
from pathlib import Path
from datetime import datetime, date, timedelta
import duckdb
import pandas as pd
import numpy as np
from pyproj import Transformer

## 좌표값 정확성 검증 -> 널값, 99999값등 제거 
## 좌표값 중에 x,y를 비교해서 아닌 게 있으면 바꿔줘서 다시 저장함
# 몇 개 바꿨는지 확인
# 다 되면 _x,y로 마크 추가하기

## 세 가지 데이터에 대해서 다 검증이 되면 touch로 붙이기
## 'output/purpose_transport/month/month_*.parquet'
## 데이터 duckDB로 불러오기
## x가 100보다 큰 경우 , y 가 100보다 작은 경우 돌리고 
## 5179로 변환해서 다시 parquet으로 저장


# 좌표값 변환

output_base = Path('output/purpose_transport')

months = ['202501', '202502', '202503', '202504', '202505', '202506']
work_types = ['residence', 'office']

con = duckdb.connect()
con.execute("SET memory_limit='100GB'")

def is_duckdb_interrupt(e:Exception) -> bool:
    name = e.__class__.__name__.lower()
    msg= (str(e) or "").lower()
    return ("interrupt" in name or "interrupted" in msg)

# 월 순회
for month in months:
    # 타입 순회
    for work_type in work_types:
        try:
            # corrected 파일 존재할 시 생략
            if Path(output_base/f'{month}'/f'{month}_{work_type}_windowed_transport_corrected.parquet').exists():
                print(f"{month}-{work_type} already exists")
                continue
            print(f"{month}-{work_type} processing")
            con.execute(f'''
                COPY 
                (SELECT
                    운행일자
                    , 가상카드번호
                    , 정류장ID
                    , hour
                    , type
                    , 지역코드
                    , 교통수단구분
                    , 정류장명칭
                    -- 좌표 뒤바뀐 경우 체크
                    , CASE WHEN y > 100 THEN 1
                        ELSE 0 END AS y_cnt
                    , CASE WHEN x < 100 THEN 1
                        ELSE 0 END AS x_cnt
                    , CASE WHEN y > 100 THEN x
                        ELSE y END AS y
                    , CASE WHEN x < 100 THEN y
                        ELSE x END AS x
                FROM '{output_base}/{month}/{month}_{work_type}_windowed_transport.parquet'
                ) TO '{output_base}/{month}/{month}_{work_type}_windowed_transport_corrected.parquet'
            ;''')
    
            counts = con.execute(f""" select 
                                SUM(y_cnt) as y_sum
                                , SUM(x_cnt) as x_sum
            from read_parquet('{output_base}/{month}/{month}_{work_type}_windowed_transport_corrected.parquet')
            """).df()
            sum_y, sum_x = counts.loc[0, 'y_sum'], counts.loc[0, 'x_sum']
            print(f'[SUC] {month}-{work_type} successed, x: {sum_x}, y: {sum_y} changed')
        except Exception as e:
            if is_duckdb_interrupt(e):
                raise KeyboardInterrupt from e 
            print(f"[FAIL] {month}-{work_type}: {e}")

        