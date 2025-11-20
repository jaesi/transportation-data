import argparse, os, csv, time, traceback, sys
from pathlib import Path
from datetime import datetime, date, timedelta
import duckdb

from utils import * 

BASE = Path("output")
STATUS_LOG =Path("output/log_status.csv")

def ensure(path):
    path.mkdir(parents=True, exist_ok=True)
    
def success_mark(dt): 
    return BASE/"od_agg"/"final"/f"{dt}"/"_SUCCESS"
    
def has_success(dt): 
    return success_mark(dt).exists()

def date_range(s: date, e: date):
    d = s
    while d <= e:
        yield d
        d += timedelta(days=1)

def atomic_swap(dt):
    
    stage = BASE/"od_agg"/"staging"/f"{dt}"
    final = BASE/"od_agg"/"final"/f"{dt}"
    
    if final.exists():
        os.system(f"rm -rf {final}")
    os.rename(stage, final)
    success_mark(dt).touch()

def append_status(row):
    write_header = not STATUS_LOG.exists()
    with STATUS_LOG.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["dt", "status", "total_length", "total_transport", 
                                          "join_ratio", "elapsed_sec", "message"])
        if write_header: 
            w.writeheader()
        w.writerow(row)

def process_one_day(dt: str, mem_gb = 100):
    # parse datetime
    d = date.fromisoformat(dt)
    yyyymm = d.strftime('%Y%m')
    yyyymmdd = d.strftime('%Y%m%d')
    
    # Path setting
    stage = BASE/"od_agg"/"staging"/f"{dt}"
    ensure(stage.parent)
    if stage.exists():
        os.system(f"rm -rf {stage}")
    ensure(stage)
    con = duckdb.connect(database='myanalysis.db')
    con.execute(f"SET memory_limit='{mem_gb}GB'")
    con.execute("PRAGMA temp_directory='/tmp';")
    # con.execute("PRAGMA memory_limit='100GB';")

    # 1) Object Transport
    con.execute('''
    CREATE TEMP TABLE o AS
    SELECT 운행일자
        , 가상카드번호
        , 트랜잭션ID
        -- 첫 승차/ 마지막 하차
        , arg_min(ltrim(정산사승차정류장ID, '0'), 승차일시) AS 승차정류장ID -- 앞자리 0 제거
        , arg_max(ltrim(정산사하차정류장ID, '0'), 하차일시) AS 하차정류장ID
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
    FROM read_csv('import_data/TB_KTS_DWTCD_METROPOLITAN/'||?||'/TB_KTS_DWTCD_METROPOLITAN_'||?||'.csv', 
        types= {'정산사승차정류장ID':'VARCHAR', '정산사하차정류장ID':'VARCHAR', '정산사노선ID':'VARCHAR', '가상카드번호':'VARCHAR'})
    GROUP BY 운행일자, 가상카드번호, 트랜잭션ID
    HAVING MIN(승차일시) IS NOT NULL AND MAX(하차일시) IS NOT NULL
    ''', [yyyymm, yyyymmdd])
    # 2) Concat
    con.execute('''
    CREATE TEMP TABLE tall AS
    SELECT
        운행일자 AS dt
        , 승차정류장ID
        , 하차정류장ID
        , 'board' AS 승하차구분 
        , SUBSTR(승차일시::VARCHAR, 9, 2) AS hour
        , 승차지역코드
        , 하차지역코드
        , 승차교통수단구분
        , 하차교통수단구분
        , 가상카드번호
        , 총이동거리
        , 총탑승시간
        , 최대환승건수
    FROM o
    UNION ALL
    SELECT
        운행일자 AS dt
        , 승차정류장ID
        , 하차정류장ID
        , 'alight' AS 승하차구분 
        , SUBSTR(하차일시::VARCHAR, 9, 2) AS hour
        , 승차지역코드
        , 하차지역코드
        , 승차교통수단구분
        , 하차교통수단구분
        , 가상카드번호
        , 총이동거리
        , 총탑승시간
        , 최대환승건수
    FROM o
    ''')
    
    # 3) Aggregate
    con.execute(f'''
    COPY (
        WITH agg AS (
            SELECT 
            dt, 승차정류장ID, 하차정류장ID, 승하차구분, hour
            , 승차지역코드, 하차지역코드, 승차교통수단구분, 하차교통수단구분
            , COUNT(*) AS 통행량
            , COUNT(DISTINCT(가상카드번호)) AS 통행명수
            , AVG(총이동거리) AS 평균이동거리
            , MEDIAN(총이동거리) AS 중위이동거리
            , AVG(총탑승시간) AS 평균탑승시간
            , MEDIAN(총탑승시간) AS 중위탑승시간
            , AVG(최대환승건수) AS 평균환승건수
            , MEDIAN(최대환승건수) AS 중위환승건수
        FROM tall
        GROUP BY 1,2,3,4,5,6,7,8,9
        ),
        -- 정류장 정보 붙이기
        location AS (
            SELECT 지역코드
                , 교통수단구분
                , ltrim(정류장ID, '0') AS 정류장ID
                , 정류장명칭
                , 정류장GPSY좌표 AS y
                , 정류장GPSX좌표 AS x
            FROM read_csv('import_data/TB_KTS_STTN/'||?||'/TB_KTS_STTN_'||?||'.csv',
            types={{'정류장ID':'VARCHAR'}})
        )
        SELECT 
                agg.dt
                , agg.승차정류장ID
                , agg.하차정류장ID
                , agg.승하차구분
                , agg.hour::INT AS hour
                , agg.승차지역코드
                , agg.하차지역코드
                , agg.승차교통수단구분
                , agg.하차교통수단구분
                , agg.통행량
                , agg.통행명수
                , agg.평균이동거리
                , agg.중위이동거리
                , agg.평균탑승시간
                , agg.중위탑승시간
                , agg.평균환승건수
                , agg.중위환승건수
                , l1.정류장명칭 AS 승차정류장명칭
                , l1.y AS 승차_y
                , l1.x AS 승차_x
                , l2.정류장명칭 AS 하차정류장명칭
                , l2.y AS 하차_y
                , l2.x AS 하차_x
        FROM agg
        JOIN location l1
             ON agg.승차정류장ID = l1.정류장ID
             AND agg.승차지역코드 = l1.지역코드
             AND agg.승차교통수단구분 = l1.교통수단구분
        JOIN location l2
             ON agg.하차정류장ID = l2.정류장ID
             AND agg.하차지역코드 = l2.지역코드
             AND agg.하차교통수단구분 = l2.교통수단구분
    ) TO '{stage}/agg_result.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    ''', [yyyymm, yyyymmdd])
    
    # 4) Quality Check
    qa_df = con.execute(f'''
        SELECT 
        COUNT(*) AS total_len
        , COALESCE(SUM(통행량), 0)::BIGINT AS total_trans
        , COALESCE(COUNT(승차_y), 0)::BIGINT AS total_y
        , ROUND(COALESCE(COUNT(승차_y), 0)/COUNT(*)*100 ,1) AS join_ratio
        FROM read_parquet("{stage}/agg_result.parquet")
    ''').df()

    tot_len = qa_df.loc[0,'total_len']
    tot_trans = qa_df.loc[0,'total_trans']
    tot_y = qa_df.loc[0,'total_y']
    join_ratio = qa_df.loc[0,'join_ratio']

    if tot_trans == 0 or join_ratio < 90:
        raise RuntimeError(f"validation failed tot_length={tot_len}, join_ratio={join_ratio}%")
    return tot_len, tot_trans, tot_y, join_ratio

def is_duckdb_interrupt(e:Exception) -> bool:
    name = e.__class__.__name__.lower()
    msg= (str(e) or "").lower()
    return ("interrupt" in name or "interrupted" in msg)
    
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--mem_gb", type=int, default=100)
    args=ap.parse_args()

    s = datetime.strptime(args.start, "%Y-%m-%d").date()
    e = datetime.strptime(args.end, "%Y-%m-%d").date()

    for d in date_range(s, e):
        dt = d.isoformat()
        t0 = time.time()
        try:
            if args.resume and has_success(dt):
                continue
            tot_len, tot_trans, tot_y, join_ratio = process_one_day(dt, mem_gb=args.mem_gb)
            atomic_swap(dt)
            append_status({"dt":dt, "status": "ready", "total_length":tot_len, 
                           "total_transport":tot_trans, "join_ratio":join_ratio,
            "elapsed_sec":round(time.time()-t0, 2), "message": ""})
            print(f"[SUCCESS] {dt} elapsed_sec: {round(time.time()-t0, 2)} tot_length:{tot_len} tot_transport:{tot_trans} tot_join:{tot_y} join_ratio:{join_ratio}%")
        except KeyboardInterrupt:
            print("--stopping cleanly", flush=True)
            raise
        except Exception as e:
            if is_duckdb_interrupt(e):
                raise KeyboardInterrupt from e 
            append_status({"dt":dt, "status":"failed", "total_length": "",
                           "total_transport":"", "join_ratio":"",
                           "elapsed_sec":round(time.time()-t0,2),
                           "message": (str(e) or "")[:400]})
            print(f"[FAIL] {dt} {e}")
            continue

if __name__ == "__main__":
    main()