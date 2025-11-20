import os, time, argparse
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from multiprocessing import Pool
import psutil
from pyproj import Transformer
from tqdm import tqdm 
from pathlib import Path

from config import CONFIG
from utils import db_connection

class BatchProcessor:
    def __init__(self):
        self.con = db_connection(read_only=True)

    def process_batch(self, batch_id):
        """단일 배치 처리"""
        checkpoint_file = f"{CONFIG['CHECKPOINT_DIR']}/batch_{batch_id:02d}.parquet"

        # 이미 처리된 케이스
        if os.path.exists(checkpoint_file):
            print(f" Batch {batch_id} already exists")
            return

        print(f" Processing batch {batch_id}...")

        # 1. 배치의 카드 리스트 
        cards = self.con.execute(f"""
            SELECT 가상카드번호 
            FROM valid_cards WHERE batch_id = {batch_id}
        """).df()['가상카드번호'].tolist()

        if len(cards) == 0:
            print(f" !Batch {batch_id} is empty")
            return

        # 2. 주거지/직장지 분석
        residence_results = self._analyze_pattern(cards, 'residence')
        office_results = self._analyze_pattern(cards, 'office')

        # 3. 결과를 행 단위로 변환 (정규화) 
        all_rows = []

        # 주거지 정류장들 
        for card_id, info in residence_results.items():
            stops_df = info['stops_df']
            
            for _, row in stops_df.iterrows():
                all_rows.append({
                    'card_id':card_id,
                    'stop_id':row['정류장ID'],
                    'stop_name':row['정류장명칭'],
                    'region_code':row['지역코드'],
                    'transport_type':row['교통수단구분'],
                    'location_type':'residence',
                    'confidence':info['confidence'],
                    'cluster_size':info['cluster_size'],
                    'total_trips':info['trips']
                })

        # 직장지 정류장들
        for card_id, info in office_results.items():
            stops_df = info['stops_df']
            
            for _, row in stops_df.iterrows():
                all_rows.append({
                    'card_id':card_id,
                    'stop_id':row['정류장ID'],
                    'stop_name':row['정류장명칭'],
                    'region_code':row['지역코드'],
                    'transport_type':row['교통수단구분'],
                    'location_type':'office',
                    'confidence':info['confidence'],
                    'cluster_size':info['cluster_size'],
                    'total_trips':info['trips']
                })

        # 저장
        if len(all_rows) > 0:
            df_result = pd.DataFrame(all_rows)
            df_result.to_parquet(checkpoint_file, index=False)
            print(f" Batch {batch_id} done: {len(all_rows)} rows"
                  f"(home: {len([r for r in all_rows if r['location_type']=='residence'])},"
                  f"work: {len([r for r in all_rows if r['location_type'] =='office'])}")
        else:
            print(f" ! Batch {batch_id} has no valid results")

    def _analyze_pattern(self, card_ids, pattern_type):
        " 주거지/직장지 패턴 분석"
        card_list = "','".join(card_ids)
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)
        # 1. 배치 해당 데이터 로드
        data = self.con.execute(f"""
            SELECT 가상카드번호
                    , 정류장ID
                    , 지역코드
                    , 교통수단구분
                    , 정류장명칭
                    , x
                    , y
            FROM read_parquet('{CONFIG['DATA_DIR']}/*/*_{pattern_type}_windowed_transport_corrected.parquet')
            WHERE 가상카드번호 IN ('{card_list}')
        """).df()

        results = {}

        # 2. 카드 번호 순회: DBSCAN으로 클러스터 산출

        data_groupby = data.groupby('가상카드번호', sort=False)
        
        for card_id, card_data in tqdm(data_groupby, total=data_groupby.ngroups):

            try:
                if len(card_data) < CONFIG['DBSCAN_MIN_SAMPLES']:
                    continue
                    
                card_data = card_data.dropna(subset=['x', 'y']).copy()
                # 좌표 변환 4326 -> 5179
                x_5179, y_5179 = transformer.transform(card_data['x'].values, card_data['y'].values)
    
                # NaN값 드랍
                valid_mask = ~(np.isnan(x_5179) | np.isnan(y_5179))
    
                # 남는게 없으면 continue
                if len(valid_mask) < 1:
                    continue
                
                # DBSCAN
                coords_5179 = np.column_stack([x_5179[valid_mask], y_5179[valid_mask]])
                # 최종 array가 0인 경우 필터
                if len(coords_5179) == 0:
                    continue
                clustering = DBSCAN(
                    eps=CONFIG['DBSCAN_EPS'],
                    min_samples=CONFIG['DBSCAN_MIN_SAMPLES']
                ).fit(coords_5179)
    
                labels = clustering.labels_
    
                # 노이즈만 있을 경우 스킵
                if len(labels[labels>=0]) == 0:
                    continue
    
                # 가장 큰 클러스터 검출
                unique_labels, counts = np.unique(labels[labels>=0], return_counts=True)
                main_cluster = unique_labels[np.argmax(counts)]
    
                # 메인 클러스터 해당 데이터프레임 copy
                cluster_mask = labels == main_cluster
                cluster_data = card_data[cluster_mask].copy()
    
                # 정류장별로 유니크하게 중복 제거
                cluster_stops_df = cluster_data[['정류장ID', '정류장명칭', '지역코드', '교통수단구분']].drop_duplicates(subset=['정류장ID', '지역코드', '교통수단구분'])
                
                cluster_size = np.sum(cluster_mask)
                confidence = round(cluster_size/len(card_data),2)
    
                # 신뢰도 기준
                print(f"\r    card_id: {card_id[:5]} Done")
                results[card_id] = {
                   'stops_df':  cluster_stops_df,
                    'confidence': confidence,
                    'trips': len(card_data),
                    'cluster_size': cluster_size
                }
            except Exception as e:
                print(f"[Error] card_id: {card_id} \n error: {e}")
        return results

def process_wrapper(batch_id):
    "멀티프로세싱 래퍼"
    processor = BatchProcessor()
    processor.process_batch(batch_id)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', type=int)
    args = parser.parse_args()
    
    # 단일 배치 처리
    if args.batch is not None:
        processor = BatchProcessor()
        processor.process_batch(args.batch)
        return

    # 전체 배치 순차적 처리
    print(f"Processing all batches...", flush=True)
    
    for batch_id in range(CONFIG['NUM_BATCHES']):
        # 배치 체크포인트가 있을 때 스킵처리
        if Path(f"{CONFIG['CHECKPOINT_DIR']}/batch_{batch_id:02d}.parquet").exists():
            print(f"    [SKIP] BatchId-{batch_id:02d} is already there.")
        t0 = time.time()
        processor = BatchProcessor()
        processor.process_batch(batch_id)
        print(f"\n All batches processed elapsed time: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()