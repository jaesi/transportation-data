'''
각 월의 대표 일의 정류장 정보를 가져와서 x, y 좌표 뒤바뀐 것 수정하고, 
Grid화 해서 돌려주는 코드
향후 재사용성을 위해 분리하여 작성

[pseudo]
input: 월의 대표일(15일)의 정류장 데이터
output: 정제된 정류장 데이터

loop: 
- 'YYYYmm' 형식으로 입력
- 디렉토리에 parquet파일로 저장
'''

import argparse, time, os
from pathlib import Path
from pyproj import Transformer
import numpy as np
import pandas as pd

from config import CONFIG
from utils import db_connection, between_months, to_grid_vec

transformer = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args = ap.parse_args()

    # 시작, 끝 달 가져오기
    start = int(args.start)
    end = int(args.end)

    months = between_months(start, end)

    # db initialize
    con = db_connection()
    for month in months:
        output_path = f'output/station_cleansed/{month}/{month}15_station_cleansed.parquet'
        os.makedirs(Path(output_path).parent, exist_ok=True)
        print(f"{month}15 station data cleasing...")
        station_corrected = con.execute(f"""
            -- 1. x, y 좌표 다른 것 수정
            SELECT 지역코드
                , 교통수단구분
                , (정류장ID::BIGINT)::VARCHAR AS 정류장ID
                , 정류장명칭
                , CASE WHEN 정류장GPSY좌표 > 100 THEN 정류장GPSX좌표
                    ELSE 정류장GPSY좌표 END AS y
                , CASE WHEN 정류장GPSX좌표 < 100 THEN 정류장GPSY좌표
                    ELSE 정류장GPSX좌표 END AS x
            FROM read_csv('import_data/TB_KTS_STTN/{month}/TB_KTS_STTN_{month}15.csv')
            WHERE 정류장GPSX좌표 IS NOT NULL 
                AND 정류장GPSY좌표 IS NOT NULL
                AND 정류장GPSX좌표 < 1000
                AND 정류장GPSY좌표 < 1000
            """).df()
        
        # 5179로 변환 
        x, y = station_corrected.x.to_numpy(float), station_corrected.y.to_numpy(float)
        x_5179, y_5179 = transformer.transform(x, y)
        station_corrected['x_5179'], station_corrected['y_5179'] = x_5179, y_5179
        
        # 그리드화
        station_corrected['grid_id'] = to_grid_vec(x_5179, y_5179)
        station_corrected = station_corrected[['정류장ID', '정류장명칭', '지역코드', '교통수단구분', 'x_5179', 'y_5179', 'grid_id']]

        # 파일로 저장 
        station_corrected.to_parquet(output_path)
        print(f"    [SUC] {month}15 station cleansed!")
                                                                                 
if __name__ == '__main__':
    main()





        