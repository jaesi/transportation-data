""" 
월별로 쪼개져 있던 테이블의 결과 통합 
"""

from config import CONFIG
from utils import db_connection

def main():
    con = db_connection()

    print("Merging checkpoint files...")

    # 1. 체크포인트 통합 
    con.execute(f"""
        CREATE OR REPLACE TABLE od_mapping AS
        SELECT * FROM read_parquet('{CONFIG['CHECKPOINT_DIR']}/batch_*.parquet')
    """)

    # 2. 간단한 통계 확인
    stats = con.execute("""
        SELECT
            COUNT(*) as total_rows
            , COUNT(DISTINCT card_id) as unique_cards
            , COUNT(CASE WHEN location_type = 'residence' THEN 1 END) as residence_rows
            , COUNT(CASE WHEN location_type = 'office' THEN 1 END) as work_rows
        FROM od_mapping
    """).df()

    print("\n===Results===")
    print(stats.to_string(index=False))

    # 3. Parquet 저장
    con.execute(f"""
        COPY od_mapping
        TO '{CONFIG['OUTPUT_DIR']}/card_id_work_od_mapping.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print(f"\n Saved to {CONFIG['OUTPUT_DIR']}/card_id_work_od_mapping.parquet")
    con.close()

if __name__ == '__main__':
    main()