"""
(추후 분석 용도)
월별 목적통행 테이블에 카드별 정류장 직주 정보를 조인하기

[세부 과정]
월별 목적통행 테이블 불러와서
카드별 정류장 직주 정보 조인
1) 출근 (주거 -> 직장 정류장) 케이스
2) 퇴근 (직장 -> 주거 정류장) 케이스
UNION ALL로 행 결합

[입력 형식]
'YYYYmm'
예시: --start 202501 --end 202506
"""
import argparse, os, holidays, duckdb
from pathlib import Path
import pandas as pd
import time 

from config import CONFIG
from utils import db_connection, between_months

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    args=ap.parse_args()

    start = int(args.start)
    end = int(args.end)

    # 사이 월값 -> 리스트
    months = between_months(start, end)

    # 환승역 정제:기존 db에서 매핑테이블 가져오기
    con_og = duckdb.connect('myanalysis.db', read_only=True)
    transfer_df = con_og.execute("""
    SELECT 정류장ID
        , 지역코드
        , 교통수단구분
        , 정류장ID_min::INT::VARCHAR as 변환정류장ID
    FROM tb_station_transfer_modified
    WHERE 정류장ID_min IS NOT NULL;""").df()
    con_og.close()
    
    con = db_connection()

    # 공휴일 불러오기
    kr_holidays= holidays.SouthKorea(years=2025)
    holiday_df = pd.DataFrame([{'holiday_date': date} for date in kr_holidays.keys() if date.month <=6])
    con.register("holidays", holiday_df)
    con.register('transfer_df', transfer_df)
    
    # 최소 신뢰도 값 설정
    min_confidence = CONFIG['MIN_CONFIDENCE']
    
    # 월 순회
    for month in months:
            
        t0 = time.time()
        print(f"{month} processing...")
        df = con.execute(f"""
        -- 1) 월별 목적통행 테이블 불러오기
        WITH purpose AS (
            SELECT *
            FROM read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet')
            ),
            
        -- 2) 카드별 정류장 직주 정보 조인
        filtered AS (
            -- 2.1) 출근 통행 (주거 -> 직장)
            SELECT p.*
                , 'morning' AS trip_type
            FROM purpose p
            -- 주거지에서 출발해서 직장지에 도착한 인원 중 출근 시간 윈도우 내에 일어난 통행
            JOIN read_parquet('output/card_id_work_od_mapping.parquet') c1
                ON p.가상카드번호 = c1.card_id
                AND c1.location_type = 'residence'
                AND c1.confidence >= {min_confidence}
                AND p.승차정류장ID = c1.stop_id
                AND p.승차지역코드 = c1.region_code
                AND p.승차교통수단구분 = c1.transport_type
                AND substr(p.승차일시::varchar, 9, 2)::int BETWEEN 6 and 10
            JOIN read_parquet('output/card_id_work_od_mapping.parquet') c2
                ON p.가상카드번호 = c2.card_id
                AND c2.location_type = 'office'
                AND c2.confidence >= {min_confidence}
                AND p.하차정류장ID = c2.stop_id
                AND p.하차지역코드 = c2.region_code
                AND p.하차교통수단구분 = c2.transport_type
                AND substr(p.하차일시::varchar, 9, 2)::int BETWEEN 6 and 10
            UNION ALL
            
            -- 2.2) 퇴근 통행 (직장 -> 주거)
            SELECT p.*
                , 'evening' AS trip_type
            FROM purpose p
            JOIN read_parquet('output/card_id_work_od_mapping.parquet') c3
                ON p.가상카드번호 = c3.card_id
                AND c3.location_type = 'office'
                AND c3.confidence >= {min_confidence}
                AND p.승차정류장ID = c3.stop_id
                AND p.승차지역코드 = c3.region_code
                AND p.승차교통수단구분 = c3.transport_type
                AND substr(p.승차일시::varchar, 9, 2)::int BETWEEN 16 and 24
            JOIN read_parquet('output/card_id_work_od_mapping.parquet') c4
                ON p.가상카드번호 = c4.card_id
                AND c4.location_type = 'residence'
                AND c4.confidence >= {min_confidence}
                AND p.하차정류장ID = c4.stop_id
                AND p.하차지역코드 = c4.region_code
                AND p.하차교통수단구분 = c4.transport_type
                AND substr(p.하차일시::varchar, 9, 2)::int BETWEEN 16 and 24
            )
        -- 분석을 위한 테이블
        SELECT COUNT(*) AS monthly_od_cnt
            , COUNT(DISTINCT(가상카드번호)) AS monthly_od_card_cnt
        FROM filtered
        """).df()

        monthly_od_cnt = df.loc[0, 'monthly_od_cnt']
        monthly_od_card_cnt = df.loc[0, 'monthly_od_card_cnt']
        print(f"    [SUC] {month} filter, elapsed time: {time.time()-t0:.1f}")
        print(f"    - info {{ monthly_od_cnt: {monthly_od_cnt}, monthly_od_card_cnt: {monthly_od_card_cnt} }}")

if __name__ == '__main__':
    main()

