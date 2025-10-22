"""
월별 목적통행 테이블에 카드별 정류장 직주 정보를 조인하기

[세부 과정]
월별 목적통행 테이블 불러와서
카드별 정류장 직주 정보 조인
1) 출근 (주거 -> 직장 정류장) 케이스
2) 퇴근 (직장 -> 주거 정류장) 케이스
UNION ALL로 행 결합

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
    con_og = duckdb.connect('myanalysis.db')
    transfer_df = con_og.execute("""
    SELECT 정류장ID
        , 지역코드
        , 교통수단구분
        , 정류장ID_min::INT::VARCHAR as 변환정류장ID
    FROM tb_station_transfer_modified
    WHERE 정류장ID_min IS NOT NULL;""").df()
    
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
        output_path = f'output/purpose_transport/work_od_filtered/{month}/{month}_workod_purpose_transport.parquet'
        os.makedirs(Path(output_path).parent, exist_ok =True)

        # 파일 존재 시 스킵
        if Path(output_path).exists():
            print(f"{month} workod_purpose_transport already exists")
            continue
            
        t0 = time.time()
        print(f"{month} processing...")
        con.execute(f"""
        COPY(
            -- 1) 월별 목적통행 테이블 불러오기
            WITH purpose AS(
                SELECT *
                FROM read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet')
                ),
                
            -- 2) 카드별 정류장 직주 정보 조인
            filtered AS(
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
                ),
                
            -- 3. 환승역 정제
            transfer_matching AS (
                SELECT f.운행일자
                    , f.가상카드번호
                    , f.트랜잭션ID
                    , CASE WHEN t1.정류장ID IS NOT NULL THEN t1.변환정류장ID 
                        ELSE f.승차정류장ID::VARCHAR END AS 승차정류장ID
                    ,CASE WHEN t2.정류장ID IS NOT NULL THEN t2.변환정류장ID 
                        ELSE f.하차정류장ID::VARCHAR END AS 하차정류장ID
                    , f.승차일시
                    , f.하차일시
                    , f.승차지역코드
                    , f.하차지역코드
                    , f.승차교통수단구분
                    , f.하차교통수단구분
                    , f.총이동거리
                    , f.총탑승시간
                    , f.최대환승건수
                    , f.trip_type
                FROM filtered f
                -- 승차 매핑
                LEFT JOIN transfer_df t1
                    ON f.승차정류장ID::VARCHAR = t1.정류장ID::VARCHAR
                    AND f.승차지역코드 = t1.지역코드
                    AND f.승차교통수단구분 = t1.교통수단구분
                -- 하차 매핑
                LEFT JOIN transfer_df t2
                    ON f.하차정류장ID::VARCHAR = t2.정류장ID::VARCHAR
                    AND f.하차지역코드 = t2.지역코드
                    AND f.하차교통수단구분 = t2.교통수단구분
            )
            
            -- 4. 집계
            SELECT t.승차정류장ID
                , t.승차지역코드
                , t.승차교통수단구분
                , t.하차정류장ID
                , t.하차지역코드
                , t.하차교통수단구분
                , t.trip_type
                , COUNT(*) AS 월_총_통행량
                , AVG(t.총이동거리) AS 이동거리_평균
                , MEDIAN(t.총이동거리) AS 이동거리_중위
                , AVG(t.총탑승시간) AS 탑승시간_평균
                , MEDIAN(t.총탑승시간) AS 탑승시간_중위
            FROM transfer_matching t
            -- 공휴일, 주말 제거
            LEFT JOIN holidays h
                ON STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE = h.holiday_date
            WHERE DAYOFWEEK(STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE) NOT IN (0,6) -- 일:0 토:6
                AND h.holiday_date IS NULL
            GROUP BY 1,2,3,4,5,6,7
            )
            TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"    [SUC] {month} filter&aggregated, elapsed time: {time.time()-t0:.1f}")

if __name__ == '__main__':
    main()

