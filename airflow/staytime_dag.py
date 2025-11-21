"""
# 교통카드 체류시간 데이터 후처리
- 해당 dag프로세스는 데이터 안심구역에서 반출 후 S3에 업로드된 원본 통근OD Parquet 파일이 있어야 정상 작동

"""

import io, tempfile
import pendulum
from typing import List
import logging
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# -------------- 전역 설정 --------------
# S3 설정
AWS_CONN_ID = 's3_bv_dropbox'
S3_BUCKET_NAME = 'bv-dropbox'

# 업로드할 DB 접속 정보
POSTGRES_CONN_ID = 'dn_airflow'
SCHEMA = 'transportation'

# DAG 월 미입력 시 기본값
DEFAULT_MONTH = ["202407"]

# 로그 설정
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 다음달 함수 헬퍼 함수
def _get_next_month_yyyymm(month: str) -> str:
    year=int(month[:4])
    mon=int(month[4:6])
    if mon == 12:
        return f"{year+1}01"
    else:
        return f"{year}{mon+1:02d}"

@dag(
    dag_id="교통카드_체류시간_후처리",
    start_date=pendulum.datetime(2025,11,19,tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["교통카드"],
    default_args={"owner":"문재식"}
)

def transportation_stay_time_process():

    @task
    def load_and_transform_s3_parquet(month: str) -> str:
        """
        s3에서 parquet을 arrow로 로드하고 
        1) 그리드 좌표 변환
        2) 컬럼명 변환 
        수행한 뒤 
        최종 스키마에 맞게 변환하여 반환

        params: 
            month: str - 처리할 월 (YYYYMM)

        return s3_file_path - str - 처리된 파일이 저장된 S3 경로

        """
        import duckdb
        import pyarrow.parquet as pq

        # 그리드 mapping list 사전 설정
        X_MAPPING = list("ABCDEFG")
        Y_MAPPING = list("BCDEFGH")
        x_list = str(X_MAPPING)
        y_list = str(Y_MAPPING)

        # S3 훅 생성
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_prefix = f"교통카드/체류시간/{month}"
        s3_file_key = f"{s3_prefix}/{month}_metropolitan_duration_time.parquet"

        ### 파일 로드 
        # 1) S3 로드 
        obj = s3_hook.get_key(key=s3_file_key, bucket_name=S3_BUCKET_NAME)
        raw_bytes = obj.get()['Body'].read()

        # 2) pyarrow로 메모리상에서 parquet -> Arrow Table
        source_table = pq.read_table(io.BytesIO(raw_bytes))

        # 3) duckDB에 Arrow table 등록
        con = duckdb.connect()
        con.register("src", source_table)
        
        # 데이터 로드 및 변환
        arrow_table = con.execute(f"""
            SELECT operation_date       AS standard_date,
                station_id,
                station_name,
                admin_code              AS station_legaldong_code,
                station_type,
                -- 영문 그리드 -> 한글 변환
                CASE
                    WHEN cluster_id IS NOT NULL
                    AND length(cluster_id) >= 12
                    THEN
                        (
                            CASE substr(cluster_id,1,1)
                                WHEN 'A' THEN '가'
                                WHEN 'B' THEN '나'
                                WHEN 'C' THEN '다'
                                WHEN 'D' THEN '라'
                                WHEN 'E' THEN '마'
                                WHEN 'F' THEN '바'
                                WHEN 'G' THEN '사'
                                WHEN 'H' THEN '아'
                            END || 
                            CASE substr(cluster_id,2,1)
                                WHEN 'A' THEN '가'
                                WHEN 'B' THEN '나'
                                WHEN 'C' THEN '다'
                                WHEN 'D' THEN '라'
                                WHEN 'E' THEN '마'
                                WHEN 'F' THEN '바'
                                WHEN 'G' THEN '사'
                                WHEN 'H' THEN '아'
                            END ||
                            substr(cluster_id, 3)
                        )
                    ELSE NULL
                END                     AS station_grid_id,
                duration_type           AS stay_type,
                duration_count          AS stay_count,
                total_duration_time*60  AS total_stay_time,     -- 분단위 -> 초단위 복원
                duration_median_time*60 AS median_stay_time,    -- 분단위 -> 초단위 복원
            -- 그리드 -> 좌표 변환
            ((list_position({x_list}, substr(cluster_id, 1, 1)) - 1 + 7) * 100000 + CAST(substr(cluster_id, 3, 5) AS BIGINT)) AS station_x,
            ((list_position({y_list}, substr(cluster_id, 2, 1)) - 1 + 14) * 100000 + CAST(substr(cluster_id, 8, 5) AS BIGINT)) AS station_y
            FROM src
        """).arrow()


        # 변환된 데이터 S3에 저장
        output_buffer = io.BytesIO()
        pq.write_table(arrow_table, output_buffer)
        output_buffer.seek(0)
        output_s3_path = f"교통카드/체류시간/tmp/{month}_metropolitan_stay_time_processed.parquet"
        
        # 디렉토리 생성

        s3_hook.load_file_obj(
            file_obj=output_buffer,
            key=output_s3_path,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        logger.info(f"Transformed file uploaded to s3://{S3_BUCKET_NAME}/{output_s3_path}")
        return output_s3_path


    @task
    def load_to_db(month:str, output_s3_path: str) -> None:
        """
        params: output_s3_path - str - 처리된 파일이 저장된 S3 경로

        return
            None
        """
        import pyarrow.parquet as pq
        import pyarrow.csv as pacsv
        from io import BytesIO

        # Postgres 훅 생성
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        # 임시테이블 생성
        create_temp_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tb_metropolitan_stay_time_stage (
            standard_date           varchar(8),
            station_id              varchar(20),
            station_name            varchar(100),
            station_legaldong_code  varchar(10),
            station_type            varchar(1),
            station_grid_id         varchar(12),
            stay_type               varchar(5),
            stay_count              numeric(6,0),
            total_stay_time         numeric(11,0),
            median_stay_time        numeric(5,0),
            station_x               numeric(11,0),
            station_y               numeric(11,0)
            );
        """
        try:
            pg_hook.run(create_temp_table_sql)
            logger.info(f"Temp table {SCHEMA}.tb_metropolitan_stay_time_stage ensured.")
        except Exception as e:
            logger.error(f"Error creating table {SCHEMA}.tb_metropolitan_stay_time_stage: {e}")
            raise
        
        # 최종 테이블 (geometry 포함) 생성
        create_final_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tb_metropolitan_stay_time (
            standard_date           varchar(8),
            station_id              varchar(20),
            station_name            varchar(100),
            station_legaldong_code  varchar(10),
            station_type            varchar(1),
            station_grid_id         varchar(12),
            stay_type               varchar(5),
            stay_count              numeric(6,0),
            total_stay_time         numeric(11,0),
            median_stay_time        numeric(5,0),
            station_geometry        geometry(Point, 5179),
            PRIMARY KEY (standard_date, station_id, station_type, station_grid_id, stay_type)
            ) PARTITION BY RANGE (standard_date);
            """
        try:
            pg_hook.run(create_final_table_sql)
            logger.info(f"Final table {SCHEMA}.tb_metropolitan_stay_time ensured.")
        except Exception as e:
            logger.error(f"Error creating table {SCHEMA}.tb_metropolitan_stay_time: {e}")
            raise
        
        # 다음달 첫날까지 RANGE 설정
        next_month = _get_next_month_yyyymm(month)

        # 파티션 테이블 삭제
        drop_partition_sql = f"""
        DROP TABLE IF EXISTS {SCHEMA}.tb_metropolitan_stay_time_{month};
        """
        pg_hook.run(drop_partition_sql)
        logger.info(f"Partition table for month {month} dropped if existed.")

        # 파티션 테이블 생성
        create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.tb_metropolitan_stay_time_{month} PARTITION OF {SCHEMA}.tb_metropolitan_stay_time
        FOR VALUES FROM ('{month}01') TO ('{next_month}01');
        """

        try:
            pg_hook.run(create_partition_sql)
            logger.info(f"Partition table for month {month} ensured.")
        except Exception as e:
            logger.error(f"Error creating partition table for month {month}: {e}")
            raise
        
        # S3 Parquet 로드 -> Arrow -> csv 임시파일 
        s3_object = s3_hook.get_key(key=output_s3_path, bucket_name=S3_BUCKET_NAME)
        file_content = s3_object.get()['Body'].read()
        table = pq.read_table(BytesIO(file_content))
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False) as temp_csv:
            temp_csv_path = temp_csv.name
        # Arrow Table -> CSV
        pacsv.write_csv(table, temp_csv_path)
        logger.info(f"Temporary CSV file created at {temp_csv_path}.")   

        # 임시테이블로 데이터 적재
        copy_sql = f"""
        COPY {SCHEMA}.tb_metropolitan_stay_time_stage (
            standard_date,
            station_id,
            station_name,
            station_legaldong_code,
            station_type,
            station_grid_id,
            stay_type,
            stay_count,
            total_stay_time,
            median_stay_time,
            station_x,
            station_y
        )
        FROM STDIN WITH CSV HEADER;
        """
        try:
            pg_hook.copy_expert(sql=copy_sql, filename=temp_csv_path)
            logger.info(f"Data loaded into {SCHEMA}.tb_metropolitan_stay_time_stage from {temp_csv_path}.")
        except Exception as e:
            logger.error(f"Error loading data into {SCHEMA}.tb_metropolitan_stay_time_stage: {e}")
            raise
        # finally:
        #     # 임시파일 삭제
        #     try:
        #         os.remove(temp_csv_path)
        #         logger.info(f"Temporary file {temp_csv_path} deleted.")
        #     except OSError as e:
        #         logger.error(f"Error deleting temporary file {temp_csv_path}: {e}")

        # 최종테이블로 데이터 이동
        insert_final_sql = f"""
        INSERT INTO {SCHEMA}.tb_metropolitan_stay_time (
            standard_date,
            station_id,
            station_name,
            station_legaldong_code,
            station_type,
            station_grid_id,
            stay_type,
            stay_count,
            total_stay_time,
            median_stay_time,
            station_geometry
        )
        SELECT
            standard_date,
            station_id,
            station_name,
            station_legaldong_code,
            station_type,
            station_grid_id,
            stay_type,
            stay_count,
            total_stay_time,
            median_stay_time,
            ST_SetSRID(ST_MakePoint(station_x, station_y), 5179) AS station_geometry
        FROM {SCHEMA}.tb_metropolitan_stay_time_stage
        WHERE standard_date >= '{month}01' 
            AND standard_date < '{next_month}01'
            AND station_grid_id IS NOT NULL -- 그리드ID NULL 제외
        """

        try:
            pg_hook.run(insert_final_sql)
            logger.info(f"Data moved from {SCHEMA}.tb_metropolitan_stay_time_stage to {SCHEMA}.tb_metropolitan_stay_time.")
        except Exception as e:
            logger.error(f"Error moving data to {SCHEMA}.tb_metropolitan_stay_time: {e}")
            raise
        # 임시테이블 데이터 삭제
        try:
            pg_hook.run(f"DROP TABLE {SCHEMA}.tb_metropolitan_stay_time_stage;")
            logger.info(f"Temp table {SCHEMA}.tb_metropolitan_stay_time_stage dropped.")
        except Exception as e:
            logger.error(f"Error dropping temp table {SCHEMA}.tb_metropolitan_stay_time_stage: {e}")
            raise

        # 적재 이후 임시 파일 삭제
        try:
            s3_hook.delete_objects(bucket=S3_BUCKET_NAME, keys=[output_s3_path])
            logger.info(f"Temporary file {output_s3_path} deleted from S3.")
        except Exception as e:
            logger.error(f"Error deleting temporary file {output_s3_path} from S3: {e}")
            raise

    # 메인 프로세스
    months: List[str] = DEFAULT_MONTH
    for month in months:
        processed_s3_path = load_and_transform_s3_parquet(month)
        load_to_db(month, processed_s3_path)
    return

transportation_stay_time_dag = transportation_stay_time_process()