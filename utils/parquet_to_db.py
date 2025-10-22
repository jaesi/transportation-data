import pandas as pd
from sqlalchemy import create_engine
import time, argparse, os
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument('--parquet_file_path', type=str, required=True, help='업로드할 파퀘파일 경로')
parser.add_argument('--db_server', type=str, required=True, help='업로드할 DB - 181과 AWS 중 선택')
parser.add_argument('--schema_name', type=str, default="", help='업로드할 db의 스키마 명 설정, 미입력 시 처리하지 않음')
parser.add_argument('--table_name', type=str, required=True, help='업로드할 db의 테이블명 설정')

args = parser.parse_args()

# DB 연결 정보
if args.db_server == '181':
    HOST = os.getenv('POSTGRESQL_HOST')
    PORT = os.getenv('POSTGRESQL_PORT')
    USER = os.getenv('POSTGRESQL_ID')
    PASSWORD = os.getenv('POSTGRESQL_PASSWORD')
    DATABASE = os.getenv('POSTGRESQL_DATABASE')

elif args.db_server == 'AWS' or args.db_server == 'aws':
    # AWS RDS 연결 정보
    HOST = os.getenv("AWS_HOST")
    PORT = os.getenv("AWS_PORT")
    USER = os.getenv("AWS_USER")
    PASSWORD = os.getenv("AWS_PASSWORD")
    DATABASE = os.getenv("AWS_DATABASE")
else:
    raise ValueError("181서버 혹은 AWS서버 중 하나를 입력해야합니다.")

SCHEMA_NAME=args.schema_name if args.schema_name != "" else None
TABLE_NAME=args.table_name
PARQUET_FILE_PATH = args.parquet_file_path

print(f"[{time.strftime('%H:%M:%S')}] Parquet 파일 로드 시작...")
try:
    df = pd.read_parquet(PARQUET_FILE_PATH)
    print(f"총 {len(df)} 건의 데이터 로드 완료.")
except Exception as e:
    print(f"Parquet 파일 로드 오류: {e}")
    exit()

# 2. SQLAlchemy 엔진 연결
DB_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

try:
    engine = create_engine(DB_URL)
    print(f"[{time.strftime('%H:%M:%S')}] DB 연결 엔진 생성 완료.")
except Exception as e:
    print(f"DB 연결 엔진 생성 오류. 설정 정보를 확인하세요: {e}")
    exit()

# 3. DB 삽입
print(f"[{time.strftime('%H:%M:%S')}] 데이터베이스 삽입 시작...")

try:
    # if_exists='append'/'replace'
    # index=False: 데이터프레임의 인덱스는 테이블에 컬럼으로 저장하지 않음
    if SCHEMA_NAME:
        df.to_sql(
            schema=SCHEMA_NAME,
            name=TABLE_NAME,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10000  # 대용량 데이터 전송 시 청크 사이즈를 설정하면 안정성 증가
        )

        print(f"[{time.strftime('%H:%M:%S')}] ⭐ 총 {len(df)} 건의 데이터가 {args.db_server} 서버에 성공적으로 업로드되었습니다! ⭐")

except Exception as e:
    print("-----------------------------------------------------")
    print("데이터 삽입 중 심각한 오류 발생. 상세 정보 확인 필요.")
    print(f"SQLAlchemy 오류: {e}")
    if hasattr(e, 'orig') and hasattr(e.orig, 'pgerror'):
        # Psycopg2 (PostgreSQL)의 원본 오류 메시지 출력
        print(f"PostgreSQL 원본 오류: {e.orig.pgerror}")
    print("-----------------------------------------------------")