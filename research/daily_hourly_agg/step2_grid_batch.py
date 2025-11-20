import argparse, os, csv, time, traceback, sys
from pathlib import Path
from datetime import datetime, date, timedelta
import duckdb
import pandas as pd
import numpy as np
from pyproj import Transformer

from utils import to_grid_vec

# 좌표변환 인스턴스 생성
transformer = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)

## 제너레이터 함수로 날짜 불러오기
def day_generator(s, e):
    d = s
    while d <= e:
        yield d
        d += timedelta(days=1)

def grid_validation(dt):
    grid_val = Path(f"output/od_agg/final/{dt}/_GRID")
    return grid_val.exists()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--mem_gb", type=int, default=50)
    args=ap.parse_args()

    s = datetime.strptime(args.start, "%Y-%m-%d").date()
    e = datetime.strptime(args.end, "%Y-%m-%d").date()

    # duckDB 연결
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.mem_gb}GB'")
    
    ## 날짜별 순회
    for dt in day_generator(s, e):
        t0 = time.time()
        
        try:
            if args.resume and grid_validation(dt):
                continue
            ## 파일 불러오기
            file_path = Path(f"output/od_agg/final/{dt}/agg_result.parquet")
            df = con.execute(f'''
                SELECT *
                FROM read_parquet('output/od_agg/final/{dt}/agg_result.parquet');
            ''').df()

            # NULL 값 제거 
            df.dropna(subset=['승차_x', '승차_y', '하차_x', '하차_y'], inplace=True)
            
            ## x, y 값 바뀐 값들 복구
            y1, x1 = df.승차_y.to_numpy(float), df.승차_x.to_numpy(float)
            y2, x2 = df.하차_y.to_numpy(float), df.하차_x.to_numpy(float)
            mask1 = (x1<100)|(y1>100)
            mask2 = (x2<100)|(y2>100)
            x1_new, y1_new = np.where(mask1, y1, x1), np.where(mask1, x1, y1)
            x2_new, y2_new = np.where(mask2, y2, x2), np.where(mask2, x2, y2)
            
            ## 좌표 변환
            al_x, al_y \
                = transformer.transform(x1_new, y1_new)
            bo_x, bo_y \
                = transformer.transform(x2_new, y2_new)

            df_grid = df.copy()
            ## 그리드컬럼 추가
            df_grid['승차_gridid'] = to_grid_vec(al_x, al_y)
            df_grid['하차_gridid'] = to_grid_vec(bo_x, bo_y)
            ## 기존 컬럼 드랍
            df_grid.drop(columns = ['승차_x', '승차_y', '하차_x', '하차_y'], inplace=True)
        
            # quality check
            board_grid_rate = round(df_grid.승차_gridid.notna().sum()/df_grid.shape[0] * 100, 1)
            alight_grid_rate = round(df_grid.하차_gridid.notna().sum()/df_grid.shape[0] * 100, 1)

            if board_grid_rate < 90 or alight_grid_rate < 90:
                raise RuntimeError(f"validation failed board_grid_rate={board_grid_rate}, alight_grid_rate={alight_grid_rate}%")
                
            ## 파일 다시 저장
            con.register("grid", df_grid)
            con.execute(f'''
                COPY grid TO 'output/od_agg/final/{dt}/agg_result.parquet'
                (FORMAT PARQUET, COMPRESSION ZSTD);
            ''')
            
            ## 성공시 touch 
            (file_path.parent/"_GRID").touch()
            print(f"[SUCCESS] {dt} grid mapping / grid_rate: {board_grid_rate}%{alight_grid_rate}% / elapsed time: {round(time.time()-t0, 1)}s")
        except Exception as e:
            print(f"[FAIL] {dt} error accured: {e}")

if __name__ == "__main__":
    main()