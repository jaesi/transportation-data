"""
6개월 주거지, 직장지 윈도우 테이블에 동시에 존재하는 카드값의 고유값들로 테이블 생성
config파일의 duckdb 경로로 테이블 산출
배치 프로세스를 위한 1~10까지의 batch_id 부여
"""

import duckdb
from config import CONFIG
from utils import db_connection, ensure_dirs

def create_valid_cards_table():
    con = db_connection()
    print("Creating Valid_cards table...")

    con.execute(f"""
        CREATE OR REPLACE TABLE valid_cards AS
        WITH home_cards AS (
            SELECT 가상카드번호
            FROM read_parquet('{CONFIG['DATA_DIR']}/*/*_residence_windowed_transport_corrected.parquet')
        GROUP BY 가상카드번호
        HAVING COUNT(*) >= 10
        ),
        work_cards AS (
            SELECT 가상카드번호
            FROM read_parquet('{CONFIG['DATA_DIR']}/*/*_office_windowed_transport_corrected.parquet')
            GROUP BY 가상카드번호
            HAVING COUNT(*) >= 10
            )
        SELECT 가상카드번호
        FROM home_cards
        INTERSECT
        SELECT 가상카드번호
        FROM work_cards
    """)

    # 카드 수 확인
    count = con.execute("SELECT COUNT(*) FROM valid_cards").fetchone()[0]
    print(f"Valid cards: {count:,}")

    return con

def assign_batches(con):
    "배치 ID 할당"
    print(f"Assigning {CONFIG['NUM_BATCHES']} batches...")

    con.execute(f"""
        ALTER TABLE valid_cards ADD COLUMN batch_id INTEGER;

        UPDATE valid_cards
        SET batch_id = (hash(가상카드번호) % {CONFIG['NUM_BATCHES']});
    """)

    # 배치별 카드 수 확인
    batch_stats = con.execute("""
        SELECT 
            batch_id,
            COUNT(*) as card_count
        FROM valid_cards
        GROUP BY batch_id
        ORDER BY batch_id
    """).df()

    print("\n===Batch Distribution===")
    print(batch_stats.to_string(index=False))
    print(f"\nAVG crads per batch: {batch_stats['card_count'].mean():.0f}")

def main():
    ensure_dirs()

    con = create_valid_cards_table()
    assign_batches(con)

    print("\n Preparation completed")
    con.close()

if __name__ == "__main__":
    main()