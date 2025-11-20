
CONFIG = {
    #경로
    'DATA_DIR': 'output/purpose_transport',
    'DB_PATH': 'work_od.db',
    'CHECKPOINT_DIR': 'output/work_od_batch',
    'OUTPUT_DIR': 'output',

    # 배치 설정
    'NUM_BATCHES': 10,
    
    # DBSCAN
    'DBSCAN_EPS': 800,
    'DBSCAN_MIN_SAMPLES': 10,
    'MIN_CONFIDENCE': 0.5,

    # 병렬 설정
    'NUM_WORKERS': 4,

}