import duckdb as duck
import pandas as pd 
import numpy as np
import time, os, holidays
import warnings
import duckdb
from pathlib import Path

# Duckdb
con = duckdb.connect()
con.execute("SET memory_limit='50GB'")

# 공휴일 등록 - 한국 기준 
kr_holidays= holidays.SouthKorea(years=2025)
holiday_df = pd.DataFrame([{'holiday_date': date} for date in kr_holidays.keys() if date.month <=6])
con.register("holidays", holiday_df)
months = ['202501', '202502', '202503', '202504', '202505', '202506']

# DuckDB용 KeyboardInterrrupt 설정
def is_duckdb_interrupt(e:Exception) -> bool:
    name = e.__class__.__name__.lower()
    msg= (str(e) or "").lower()
    return ("interrupt" in name or "interrupted" in msg)

for month in months:
    try:
        print(f"Processing: {month}")
        output_path = f'output/purpose_transport/{month}'
        
        # 주거지 파일 존재 시: 패스
        if not Path(f"{output_path}/{month}_residence_windowed_transport.parquet").exists():
            print(f"Processing: residence windowed transport")
            ## 1. 주거지 타임 윈도우 테이블 생성 
            con.execute(f'''
                COPY (
                -- 1) 승차
                WITH board AS(
                    select t.운행일자
                        , t.가상카드번호
                        , t.승차정류장ID AS 정류장ID
                        , substr(t.승차일시::VARCHAR, 9, 2) AS hour
                        , 'board' AS type
                        , t.승차지역코드 AS 지역코드
                        , t.승차교통수단구분 AS 교통수단구분
                        , st1.정류장명칭
                        , st1.정류장GPSY좌표 AS y
                        , st1.정류장GPSX좌표 AS x
                    from read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet') t
                    LEFT JOIN holidays h
                        ON STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE = h.holiday_date
                    LEFT JOIN read_csv('import_data/TB_KTS_STTN/{month}/TB_KTS_STTN_{month}15.csv') st1
                        ON t.승차정류장ID = st1.정류장ID
                        AND t.승차지역코드 = st1.지역코드
                        AND t.승차교통수단구분 = st1.교통수단구분  
                    WHERE DAYOFWEEK(STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE) NOT IN (0,6) -- 일:0 토:6
                        AND h.holiday_date IS NULL
                        AND substr(승차일시::VARCHAR, 9, 2)::INT BETWEEN 6 AND 10
                        ),
                -- 2) 하차
                alight AS (
                    select t.운행일자
                        , t.가상카드번호
                        , t.하차정류장ID AS 정류장ID
                        , substr(t.하차일시::VARCHAR, 9, 2) AS hour
                        , 'alight' AS type
                        , t.하차지역코드 AS 지역코드
                        , t.하차교통수단구분 AS 교통수단구분
                        , st2.정류장명칭
                        , st2.정류장GPSY좌표 AS y
                        , st2.정류장GPSX좌표 AS x
                    from read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet') t
                    LEFT JOIN holidays h
                        ON STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE = h.holiday_date
                    LEFT JOIN read_csv('import_data/TB_KTS_STTN/{month}/TB_KTS_STTN_{month}15.csv') st2
                        ON t.하차정류장ID = st2.정류장ID
                        AND t.하차지역코드 = st2.지역코드
                        AND t.하차교통수단구분 = st2.교통수단구분   
                    WHERE DAYOFWEEK(STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE) NOT IN (0,6) -- 일:0 토:6
                        AND h.holiday_date IS NULL
                        AND substr(하차일시::VARCHAR, 9, 2)::INT BETWEEN 16 AND 24
                    )
                SELECT *
                FROM board
                UNION ALL
                SELECT *
                FROM alight
                )
                TO '{output_path}/{month}_residence_windowed_transport.parquet'
                (FORMAT PARQUET, COMPRESSION ZSTD);
                ''')
            print(f"    [SUC] {month} residence windowed table successfully created into parquet") 
        
        ## 2. 직장지 타임 윈도우 테이블 생성 
        ## 직장지 작업파일 있을 경우 생략됨
        if Path(f"{output_path}/{month}_office_windowed_transport.parquet").exists():
            continue
        print(f"   Processing: office windowed transport")
        con.execute(f'''
            COPY (
            WITH board AS(
                select t.운행일자
                    , t.가상카드번호
                    , t.승차정류장ID AS 정류장ID
                    , substr(t.승차일시::VARCHAR, 9, 2) AS hour
                    , 'board' AS type
                    , t.승차지역코드 AS 지역코드
                    , t.승차교통수단구분 AS 교통수단구분
                    , st1.정류장명칭
                    , st1.정류장GPSY좌표 AS y
                    , st1.정류장GPSX좌표 AS x
                from read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet') t
                LEFT JOIN holidays h
                    ON STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE = h.holiday_date
                LEFT JOIN read_csv('import_data/TB_KTS_STTN/{month}/TB_KTS_STTN_{month}15.csv') st1
                    ON t.승차정류장ID = st1.정류장ID
                    AND t.승차지역코드 = st1.지역코드
                    AND t.승차교통수단구분 = st1.교통수단구분  
                WHERE DAYOFWEEK(STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE) NOT IN (0,6) -- 일:0 토:6
                    AND h.holiday_date IS NULL
                    AND substr(승차일시::VARCHAR, 9, 2)::INT BETWEEN 16 AND 24
                    ),
            alight AS (
                select t.운행일자
                    , t.가상카드번호
                    , t.하차정류장ID AS 정류장ID
                    , substr(t.하차일시::VARCHAR, 9, 2) AS hour
                    , 'alight' AS type
                    , t.하차지역코드 AS 지역코드
                    , t.하차교통수단구분 AS 교통수단구분
                    , st2.정류장명칭
                    , st2.정류장GPSY좌표 AS y
                    , st2.정류장GPSX좌표 AS x
                from read_parquet('output/purpose_transport/{month}/{month}_purpose_transport.parquet') t
                LEFT JOIN holidays h
                    ON STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE = h.holiday_date
                LEFT JOIN read_csv('import_data/TB_KTS_STTN/{month}/TB_KTS_STTN_{month}15.csv') st2
                    ON t.하차정류장ID = st2.정류장ID
                    AND t.하차지역코드 = st2.지역코드
                    AND t.하차교통수단구분 = st2.교통수단구분   
                WHERE DAYOFWEEK(STRPTIME(t.운행일자::TEXT, '%Y%m%d')::DATE) NOT IN (0,6) -- 일:0 토:6
                    AND h.holiday_date IS NULL
                    AND substr(하차일시::VARCHAR, 9, 2)::INT BETWEEN 6 AND 10
                )
            SELECT *
            FROM board
            UNION ALL
            SELECT *
            FROM alight
            )
            TO '{output_path}/{month}_office_windowed_transport.parquet'
            (FORMAT PARQUET, COMPRESSION ZSTD);
            ''')
        print(f"    [SUC] {month} office windowed table successfully created into parquet") 
    except Exception as e:
        if is_duckdb_interrupt(e):
            raise KeyboardInterrupt from e 
        print(f"    [FAIL] {month} failed: {e}")