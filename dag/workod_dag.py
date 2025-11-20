"""
# 교통카드 통근OD 데이터 후처리 DAG
- 해당 dag프로세스는 데이터 안심구역에서 반출 후 S3에 업로드된 원본 통근OD Parquet 파일이 있어야 정상 작동

### 주요 설정 파라미터 ###
1) S3 버킷
- S3 버킷: bv-dropbox
- S3 read 경로: 교통카드/통근OD/{YYYYMM}/{YYYYMM}_workod_purpose_transport_grid.parquet

2) 업로드 대상 DB 정보
- 업로드 DB: Postgres (dataops_test_181)                    << -- 현재는 테스트용으로 실제 운영 DB로 변경 필요
- 스키마: temporary
- 테이블: tb_metropolitan_work_od (파티셔닝: standard_ym)

3) 대상 월
- Airflow Variable: transportation_target_months 혹은 DAG Run Config 의 transportation_target_months 파라미터 활용
- YYYYMM 형식의 문자열 리스트
- 미지정 시 기본값: 202506

자세한 내용은 '교통카드_통근OD_후처리.md' 문서를 참고하세요.
"""

from __future__ import annotations

import io, os
import pendulum
from typing import List, Dict
import logging
import s3fs
import boto3
import pandas as pd
import pyarrow.parquet as pq
import shapely.geometry as sg
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonVirtualenvOperator
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowSkipException
from shapely.geometry import Point

# -------------- 전역 설정 --------------
# S3 설정
AWS_CONN_ID = 's3_bv_dropbox'
S3_BUCKET_NAME = 'bv-dropbox'

# 업로드할 DB 접속 정보
POSTGRES_CONN_ID = 'dn_airflow'
SCHEMA = 'transportation'

# 그리드 변환용 매핑
X_MAPPING = list("ABCDEFG")
Y_MAPPING = list("BCDEFGH")

# DAG 월 미입력 시 기본값
DEFAULT_MONTH = ["202501"] # , "202502", "202503", "202504", "202505", "202506"

# Helper 함수
def convert_grid_to_5179(grid_id: str) -> Point | None:
    """
    단일 그리드 ID -> Point 객체 변환
    """
    if not isinstance(grid_id, str) or len(grid_id) < 12:
        return None
    try:
        x_char, y_char = grid_id[0], grid_id[1]

        x_index = X_MAPPING.index(x_char)
        y_index = Y_MAPPING.index(y_char)

        x_tail = int(grid_id[2:7])
        y_tail = int(grid_id[7:])
    except (ValueError, IndexError):
        return None
    
    x = (x_index + 7) * 100_000 + x_tail
    y = (y_index + 14) * 100_000 + y_tail
    return Point(x, y)

def apply_grid_to_5179(series: pd.Series) -> pd.Series:
    return series.apply(convert_grid_to_5179)


# ---------- DAG ----------
@task.virtualenv(
    task_id="get_korean_business_day_task", 
    requirements=['pandas', 'pendulum', 'holidays'],
    system_site_packages=False)
def get_korean_business_day(ym:str) -> int:
    """
    YYYYMM형태의 문자열을 받아 해당 월의 영업일(평일) 수를 반환함
    holidays 라이브러리를 활용하기 위해 가상환경오퍼레이터 활용
    """
    import datetime as dt
    import pandas as pd
    import calendar
    import holidays
    import logging
    logger = logging.getLogger('airflow.task')
    
    if len(ym)!=6 or not ym.isdigit():
        raise ValueError("YYYYMM형태의 문자열을 입력해야합니다.")
    year = int(ym[:4])
    month = int(ym[4:6])
    
    start_date = dt.date(year, month, 1)
    _, last_day = calendar.monthrange(year, month)
    end_date = dt.date(year, month, last_day)
    rng = pd.date_range(start=start_date, end=end_date, freq='D', tz='Asia/Seoul')
    
    is_weekday = rng.weekday < 5
    _KR_HOLIDAYS= holidays.KR(years=[year])
    holiday_ts = pd.Index([pd.Timestamp(d, tz='Asia/Seoul') for d in _KR_HOLIDAYS.keys()])
    is_holiday = rng.isin(holiday_ts)
    business_day_mask = is_weekday & ~is_holiday
    logger.debug(f"Calculated business days for {ym}: {business_day_mask.sum()}")
    
    return int(business_day_mask.sum())

@dag(
    dag_id="교통카드_통근OD_후처리",
    start_date=pendulum.datetime(2025, 11, 13, tz="Asia/Seoul"),
    schedule=None,          # 수동으로 설정
    catchup=False,
    tags=["교통카드"],
    default_args={"owner": "문재식"},
)

def transportation_data_postprocessing_2025():
    @task
    def resolve_target_months(**context) -> List[str]:
        """
        전달된 월 리스트를 반환
        """
        # 1) Run time config
        conf_months = context.get("dag_run").conf.get("transportation_target_months") if context.get("dag_run") else None

        if conf_months:
            if isinstance(conf_months, str):
                conf_months = [conf_months]
            return list(conf_months)
        # 2) Airflow Variables
        var_months = Variable.get("transportation_target_months", default_var=DEFAULT_MONTH)
        if isinstance(var_months, str):
            var_months = [m.strip() for m in var_months.split(",") if m.strip()]
        return list(var_months)
    

    @task
    def load_parquet_from_s3(ym: str) -> str:
        logger = logging.getLogger('airflow.task')
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        src_key = f"교통카드/통근OD/{ym}/{ym}_workod_purpose_transport_grid.parquet"

        s3_full_path = f"s3://{S3_BUCKET_NAME}/{src_key}"
        logger.info(f"Loading parquet from {s3_full_path}")

        try:
            # S3에 파일이 있는지 먼저 확인
            s3_hook.head_object(bucket_name=S3_BUCKET_NAME, key=src_key)
            
            logger.info(f"S3 object found. Path: {s3_full_path}")
            return s3_full_path

        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'NoSuchKey':
                logger.warning(f"S3 객체를 찾을 수 없습니다: {s3_full_path}. 이 태스크를 스킵합니다.")
                raise AirflowSkipException(f"S3 객체를 찾을 수 없습니다: {s3_full_path}")
            elif error_code == 'AccessDenied':
                logger.error(f"S3 접근 거부: {s3_full_path}. IAM 권한을 확인하세요. 오류: {e}")
                raise
            else:
                logger.exception(f"S3에서 객체를 로드하는 중 예상치 못한 오류 발생 (ClientError): {e}")
                raise
        except Exception as e:
            logger.exception(f"Parquet 파일을 S3에서 로드하는 중 예기치 않은 오류 발생: {e}")
            raise
    
    @task
    def transform_parquet(s3_parquet_path: str, ym: str, business_days: int) -> str: # bytes -> str (파일 경로)
        """
        1) Parquet -> DataFrame
        2) 그리드 -> 5179 좌표 변환 -> wkt
        3) 영문 그리드 -> 국가표준한글그리드
        4) 영업일 기반 일 평균 이용건수 계산 등
        5) 필요한 컬럼만 골라서 CSV 문자열으로 반환
        """
        logger = logging.getLogger('airflow.task')
        logger.info(f"Transforming data from S3 path: {s3_parquet_path}")
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        session = s3_hook.get_session()
        credentials = session.get_credentials()
        region_name = session.region_name
        fs_kwargs = {
            "key": credentials.access_key,
            "secret": credentials.secret_key,
            "token": credentials.token,
            "client_kwargs": {"region_name": region_name},
        }
        fs = s3fs.S3FileSystem(**fs_kwargs)

        # 1) Parquet -> DataFrame
        table = pq.read_table(s3_parquet_path, filesystem=fs)
        df = table.to_pandas()
        logger.info(f"DataFrame loaded. Shape: {df.shape}")

        # 2) 좌표 변환 (grid ID → EPSG:5179 Point)
        vec_convert = pd.Series(df["승차그리드ID"]).apply(convert_grid_to_5179)
        df["departure_station_geometry"] = vec_convert.apply(
            lambda pt: pt.wkt if pt else None
        )
        vec_convert = pd.Series(df["하차그리드ID"]).apply(convert_grid_to_5179)
        df["arrival_station_geometry"] = vec_convert.apply(
            lambda pt: pt.wkt if pt else None
        )

            
        # 3) 일평균 이용건수
        df['daily_use_count'] = (df['월_총_통행량'] / business_days).round(1)
        
        # 4) 목적명칭 매핑
        df['purpose_name'] = df['출퇴근구분'].map({'morning':'출근', 'evening':'퇴근'})
        # 5) 기준연월 컬럼 추가
        df['standard_ym'] = ym
        # 6) 영문 그리드 -> 국가표준한글그리드 변환
        grid_map = {'A':'가', 'B':'나', 'C':'다', 'D':'라', 'E':'마', 'F':'바', 'G':'사', 'H':'아'}
        df['승차그리드ID'] = df['승차그리드ID'].replace(grid_map, regex=True)
        df['하차그리드ID'] = df['하차그리드ID'].replace(grid_map, regex=True)

        # 7) 컬럼명 영문 표준화
        df_standardized = df.rename(columns={
            '승차정류장ID':'departure_station_id',
            '승차정류장명칭':'departure_station_name',
            '승차지역코드':'departure_region_code',
            '승차교통수단구분':'departure_station_type',
            '승차그리드ID':'departure_grid_id',
            '하차정류장ID':'arrival_station_id',
            '하차정류장명칭':'arrival_station_name',
            '하차지역코드':'arrival_region_code',
            '하차교통수단구분':'arrival_station_type',
            '하차그리드ID':'arrival_grid_id',
            '탑승시간_중위':'median_elapse_time',
            '탑승시간_평균':'average_elapse_time',
            '이동거리_중위':'median_distance',
            '이동거리_평균':'average_distance',
        })
        db_cols = [
            "standard_ym",
            "departure_station_id",
            "departure_station_name",
            "departure_region_code",
            "departure_station_type",
            "departure_grid_id",
            "arrival_station_id",
            "arrival_station_name",
            "arrival_region_code",
            "arrival_station_type",
            "arrival_grid_id",
            "purpose_name",
            "daily_use_count",
            "median_elapse_time",
            "average_elapse_time",
            "median_distance",
            "average_distance",
            "departure_station_geometry",
            "arrival_station_geometry",
        ]
        df_standardized = df_standardized[db_cols]

        # 7) CSV 버퍼에 WRITE 
        csv_buffer = io.StringIO()
        df_standardized.to_csv(csv_buffer, index=False, header=False, na_rep='\\N')
        csv_str = csv_buffer.getvalue()
        return csv_str

    @task
    def load_to_postgres(csv_str:str, ym:str) -> None:
        """
        `PostgresHook`을 이용해 `COPY FROM STDIN` 으로 한 번에 적재합니다.
        * 테이블이 없으면 자동 생성 (첫 실행 시 한 번만 수행)
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        logger = logging.getLogger('airflow.task')
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # 1) 테이블 존재 여부 확인 및 생성
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tb_metropolitan_work_od (
            standard_ym                 VARCHAR(6) NOT NULL,
            departure_station_id        VARCHAR(20) NOT NULL,
            departure_station_name      VARCHAR(100),
            departure_region_code       VARCHAR(20) NOT NULL,
            departure_station_type      VARCHAR(20) NOT NULL,
            departure_grid_id           VARCHAR(12),
            arrival_station_id          VARCHAR(20) NOT NULL,
            arrival_station_name        VARCHAR(100),
            arrival_region_code         VARCHAR(20) NOT NULL,
            arrival_station_type        VARCHAR(20) NOT NULL,
            arrival_grid_id             VARCHAR(12),
            purpose_name                VARCHAR(10) NOT NULL,
            daily_use_count             NUMERIC(10,1),
            median_elapse_time          NUMERIC(5,0),
            average_elapse_time         NUMERIC(5,0),
            median_distance             NUMERIC(6,0),
            average_distance            NUMERIC(6,0),
            departure_station_geometry  GEOMETRY(Point, 5179),
            arrival_station_geometry    GEOMETRY(Point, 5179),
            -- PK 추가
            CONSTRAINT pk_metropolitan_workod 
            PRIMARY KEY (standard_ym, departure_station_id, departure_region_code, departure_station_type,
            arrival_station_id, arrival_region_code, arrival_station_type, purpose_name)
        )
        PARTITION BY LIST (standard_ym)
        ;
        """
        try:
            logger.info(f"Executing CREATE PARENT TABLE DDL for {SCHEMA}.tb_metropolitan_work_od")
            pg.run(create_sql)
        except Exception as e:
            logger.error(f"테이블 생성 중 오류 발생: {e}")
            raise

        # 파티션 테이블 생성
        partition_sql = f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA}.tb_metropolitan_work_od_{ym}
            PARTITION OF {SCHEMA}.tb_metropolitan_work_od
            FOR VALUES IN ('{ym}');
        """
        try:
            logger.info(f"Executing CREATE PARTITION TABLE DDL for {SCHEMA}.tb_metropolitan_work_od_{ym}")
            pg.run(partition_sql)
        except Exception as e:
            logger.error(f"파티션 테이블 생성 중 오류 발생: {e}")
            raise

        # 기존 데이터 삭제
        truncate_sql = f"""
        TRUNCATE TABLE {SCHEMA}.tb_metropolitan_work_od_{ym};
        """
        try:
            logger.info(f"Executing TRUNCATE TABLE for {SCHEMA}.tb_metropolitan_work_od_{ym}")
            pg.run(truncate_sql)
        except Exception as e:
            logger.error(f"기존 데이터 삭제 중 오류 발생: {e}")
            raise
        
        # 2) COPY FROM STDIN 으로 데이터 적재
        copy_sql = f"""
        COPY {SCHEMA}.tb_metropolitan_work_od (
            standard_ym,
            departure_station_id,
            departure_station_name,
            departure_region_code,
            departure_station_type,
            departure_grid_id,
            arrival_station_id,
            arrival_station_name,
            arrival_region_code,
            arrival_station_type,
            arrival_grid_id,
            purpose_name,
            daily_use_count,
            median_elapse_time,
            average_elapse_time,
            median_distance,
            average_distance,
            departure_station_geometry,
            arrival_station_geometry
        )
        FROM STDIN WITH (FORMAT csv, NULL '\\N');
        """
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.copy_expert(sql=copy_sql, file=io.StringIO(csv_str))
        

    @task
    def final_success(**context):
        context["ti"].log.info("🎉모든 월에 대해 성공적으로 적재되었습니다.🎉")
        return "PROCESSING_SUCCESS"

    # 워크플로우 정의
    month_list = resolve_target_months()
    business_days = get_korean_business_day.expand(ym=month_list)
    
    s3_parquet_path = load_parquet_from_s3.expand(ym=month_list)
    csv_strings = transform_parquet.expand(s3_parquet_path=s3_parquet_path, ym=month_list, business_days=business_days)
    last_load_tasks = load_to_postgres.expand(csv_str=csv_strings, ym=month_list)
    
    last_load_tasks >> final_success()


# dag 등록
transportation_data_postprocessing_2025()