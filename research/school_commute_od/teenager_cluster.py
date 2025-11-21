"""

"""
from sklearn.cluster import DBSCAN
import pandas as pd
from pyproj import Transformer
import duckdb as duck
from tqdm import tqdm
import numpy as np

# CRS transformer: 4326 -> 5179[UTM]
transformer = Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)

# 1. DBSCAN 후 클러스터 태그 함수
# Input: home_cluster or school_cluster 
# Output: 'User_id', station_x, station_y, cluster, is_main_cluster

def preprocess_candidates(df, morning_hours=(5,10), evening_hours=(16,22)) -> (pd.DataFrame, pd.DataFrame):
    # 1) 전처리 프로세스
    # Time Window preprocessing
    morning_board = df[df['승차일시'].dt.hour.between(*morning_hours)][['가상카드번호', '승차_x', '승차_y']]\
    .rename(columns={'승차_x':'x', '승차_y':'y'})
    evening_alight = df[df['하차일시'].dt.hour.between(*evening_hours)][['가상카드번호', '하차_x', '하차_y']]\
    .rename(columns={'하차_x':'x', '하차_y':'y'})

    morning_alight = df[df['하차일시'].dt.hour.between(*morning_hours)][['가상카드번호', '하차_x', '하차_y']]\
    .rename(columns={'하차_x':'x', '하차_y':'y'})
    evening_board = df[df['승차일시'].dt.hour.between(*evening_hours)][['가상카드번호', '승차_x', '승차_y']]\
    .rename(columns={'승차_x':'x', '승차_y':'y'})

    # concat
    home_candidates = pd.concat([morning_board, evening_alight], ignore_index=True)
    school_candidates = pd.concat([morning_alight, evening_board], ignore_index=True)
    return home_candidates, school_candidates


def dbscan_cluster(df, eps=800, min_samples=10, metric='manhattan'):
    # 2) dbscan 수행
    all_results = []
    for user, g in df.groupby('가상카드번호'):
        coords = g[['x', 'y']].values
        if len(coords) < min_samples:
            continue
        labels = DBSCAN(eps=eps, min_samples=min_samples, metric=metric).fit_predict(coords)
        g = g.copy()
        g['cluster']=labels
        if (g['cluster'] != -1).sum() >0:
            main_cluster = g[g['cluster'] != -1]['cluster'].value_counts().idxmax()
            g['is_main_cluster'] = g['cluster'] == main_cluster
        else:
            g['is_main_cluster']=False
        all_results.append(g)
    return pd.concat(all_results)

def evaluate_noise_ratio(df_tagged):
    return round((df_tagged['cluster'] == -1).sum()/len(df_tagged)*100, 1)

def evaluate_cluster_ratio(df_home_tagged, df_school_tagged):
    total = 1000 
    # Unique Values set
    home_set = set(df_home_tagged.query('is_main_cluster == True').가상카드번호.unique())
    school_set = set(df_school_tagged.query('is_main_cluster == True').가상카드번호.unique())
    both_cluster_person = home_set.intersection(school_set)
    # Calculate ratio
    home_ratio = df_home_tagged.query('is_main_cluster == True').가상카드번호.nunique()/total*100
    school_ratio = df_school_tagged.query('is_main_cluster == True').가상카드번호.nunique()/total*100
    both_ratio = len(both_cluster_person)/total*100
    return home_ratio, school_ratio, both_ratio
    
# Whole Pipeline
def pipeline(df, morning_hours, evening_hours, eps, min_samples, metric):
    # 1) Time-window preprocessing
    home_candidates, school_candidates = preprocess_candidates(df, morning_hours, evening_hours)
    # 2) DBSCAN
    dbscan_tagged_home = dbscan_cluster(home_candidates, eps, min_samples, metric)
    dbscan_tagged_school = dbscan_cluster(school_candidates, eps, min_samples, metric)
    # 3) Calculate noise ratio
    home_noise_ratio = evaluate_noise_ratio(dbscan_tagged_home)
    school_noise_ratio = evaluate_noise_ratio(dbscan_tagged_school)
    # 4) Drop Duplicates: 가상카드번호와 고유한 정류장 정보만 남기고 후에 merge를 고려
    df_home_no_dupe = dbscan_tagged_home.drop_duplicates(subset=['가상카드번호', 'x', 'y'])
    df_school_no_dupe = dbscan_tagged_school.drop_duplicates(subset=['가상카드번호', 'x', 'y'])
    # 4) Evaluate Cluster ratio
    home_ratio, school_ratio, both_ratio = evaluate_cluster_ratio(df_home_no_dupe, df_school_no_dupe)

    return {'eps':eps,
        'min_samples':min_samples,
        'metric':metric,
        'morning_hour':morning_hours,
        'evening_hour':evening_hours,
        'home_ratio':home_ratio,
        'school_ratio':school_ratio,
        'both_ratio':both_ratio,
        'home_noise_ratio':home_noise_ratio,
        'school_noise_ratio':school_noise_ratio}

# pipeline Output => DataFrame
def pipeline_to_df(df, morning_hours, evening_hours, eps, min_samples, metric):
    # 1) Time-window preprocessing
    home_candidates, school_candidates = preprocess_candidates(df, morning_hours, evening_hours)
    # 2) DBSCAN
    dbscan_tagged_home = dbscan_cluster(home_candidates, eps, min_samples, metric)
    dbscan_tagged_school = dbscan_cluster(school_candidates, eps, min_samples, metric)
    # 3) Calculate noise ratio
    home_noise_ratio = evaluate_noise_ratio(dbscan_tagged_home)
    school_noise_ratio = evaluate_noise_ratio(dbscan_tagged_school)
    # 4) Drop Duplicates: 가상카드번호와 고유한 정류장 정보만 남기고 후에 merge를 고려
    df_home_no_dupe = dbscan_tagged_home.drop_duplicates(subset=['가상카드번호', 'x', 'y'])
    df_school_no_dupe = dbscan_tagged_school.drop_duplicates(subset=['가상카드번호', 'x', 'y'])
    # 4) Evaluate Cluster ratio
    home_ratio, school_ratio, both_ratio = evaluate_cluster_ratio(df_home_no_dupe, df_school_no_dupe)

    return dbscan_tagged_home, dbscan_tagged_school

# Batch Pipeline
def process_batch(batch_ids):
    batch_df = pd.DataFrame({'가상카드번호': batch_ids})
    con.register('batch_ids', batch_df)

    df = con.execute('''
        SELECT 
            c.가상카드번호,
            c.정류장ID,
            c.지역코드,
            c.교통수단구분,
            c.x,
            c.y
        FROM teenager.tb_residence_candidates_2024 c 
        JOIN batch_ids b USING (가상카드번호)
    ''').fetchdf()

    # Group -> DBSCAN
    results = []
    for uid, user_df in df.groupby('가상카드번호'):
        coords = user_df[['x', 'y']].values
        labels = DBSCAN(eps=800, min_samples = 10, metric='manhattan').fit_predict(coords)

        mask = labels != -1
        if not mask.any():
            continue
        labs, counts = np.unique(labels[mask], return_counts=True)
        top_lab = labs[np.argmax(counts)]

        pk_df = user_df[labels == top_lab][['정류장ID', '지역코드', '교통수단구분']].drop_duplicates()
        for _, row in pk_df.iterrows():
            results.append((uid, row['정류장ID'], row['지역코드'], row['교통수단구분']))

    # Turn into DataFrame
    result_df = pd.DataFrame(results, columns = ['가상카드번호', '정류장ID', '지역코드', '교통수단구분'])
    return result_df


# Batch Process Pipeline
def process_batch(batch_ids, cluster_type):
    batch_df = pd.DataFrame({'가상카드번호': batch_ids})
    con.register('batch_ids', batch_df)

    df = con.execute(f'''
        SELECT 
            c.가상카드번호,
            c.정류장ID,
            c.지역코드,
            c.교통수단구분,
            c.x,
            c.y
        FROM teenager.tb_{cluster_type}_candidates_2024 c 
        JOIN batch_ids b USING (가상카드번호)
    ''').fetchdf()

    # Drop Null values
    df = df.dropna(subset=['x', 'y'])
    
    # CRS transform
    utm_x, utm_y = transformer.transform(df['x'].values, df['y'].values)
    df['x'], df['y'] = utm_x, utm_y
    
    # Group -> DBSCAN
    results = []
    for uid, user_df in df.groupby('가상카드번호'):
        coords = user_df[['x', 'y']].values
        labels = DBSCAN(eps=800, min_samples = 10, metric='manhattan').fit_predict(coords)
        mask = labels != -1
        if not mask.any():
            continue
        labs, counts = np.unique(labels[mask], return_counts=True)
        top_lab = labs[np.argmax(counts)]

        pk_df = user_df[labels == top_lab]\
        [['정류장ID', '지역코드', '교통수단구분']].drop_duplicates()
        for _, row in pk_df.iterrows():
            results.append((uid, row['정류장ID'], row['지역코드'], row['교통수단구분']))
        
    # Turn into DataFrame
    result_df = pd.DataFrame(results, columns = 
                             ['가상카드번호', '정류장ID', '지역코드', '교통수단구분'])
    return result_df

# main-pipeline Batch process
if __name__ == '__main__':
    # Connecting Duckdb
    con = duck.connect(database='myanalysis.db', read_only=False)
    con.execute("PRAGMA temp_directory='/tmp';")
    con.execute("PRAGMA memory_limit='100GB';")
    # 각각 time_windowed table에서 가상카드번호 고유값 추출
    user_ids_residence = con.execute('''
        SELECT DISTINCT 가상카드번호 
        FROM teenager.tb_residence_candidates_2024;''')\
                        .df()['가상카드번호'].tolist()
    user_ids_school  = con.execute('''
        SELECT DISTINCT 가상카드번호 
        FROM teenager.tb_school_candidates_2024;''')\
                        .df()['가상카드번호'].tolist()
    print(len(user_ids_residence), len(user_ids_school))
    # Batch distribution
    # 가상카드번호 고유값 n개 단위 배치 생성 
    BATCH_SIZE = 10000
    residence_batch = [user_ids_residence[i:i+BATCH_SIZE] 
                       for i in range(0, len(user_ids_residence), BATCH_SIZE)]
    school_batch = [user_ids_school[i:i+BATCH_SIZE] 
                    for i in range(0, len(user_ids_school), BATCH_SIZE)]
    # reset table 
    reset_query = '''
    DROP TABLE IF EXISTS teenager.tb_cardid_dbscan_clustered;
    CREATE TABLE teenager.tb_cardid_dbscan_clustered
        (가상카드번호 VARCHAR,
        정류장ID VARCHAR,
        지역코드 VARCHAR,
        교통수단구분 VARCHAR(1),
        클러스터구분 VARCHAR);
    '''
    con.execute(reset_query)
    
    # 1st-loop: Residence Cluster
    for batch_ids in tqdm(residence_batch):
        result_df = process_batch(batch_ids, cluster_type = 'residence')
        con.register('temp_res', result_df)
        con.execute('''
            INSERT INTO teenager.tb_cardid_dbscan_clustered
            SELECT 
                가상카드번호,
                정류장ID,
                지역코드,
                교통수단구분,
                'home' AS 클러스터구분
            FROM temp_res''')
    # 2nd-loop: School Cluster
    for batch_ids in tqdm(residence_batch):
        result_df = process_batch(batch_ids, cluster_type = 'school')
        con.register('temp_res', result_df)
        con.execute('''
            INSERT INTO teenager.tb_cardid_dbscan_clustered
            SELECT 
                가상카드번호,
                정류장ID,
                지역코드,
                교통수단구분,
                'school' AS 클러스터구분
            FROM temp_res''')
        


"""
 1) “너가 가진 CV 경험 → 우리 도메인(화장품) 문제에 어떻게 적용?”

너는 YOLOv8·세그멘테이션·거리뷰 분석 등 도시/교통 기반 CV 경험이 강함.
면접관은 이것을 화장품 산업의 실제 문제로 바로 연결할 수 있는지 테스트한다.

예상 질문

“CV로 화장품 도메인의 어떤 문제들을 해결할 수 있다고 보나요?”

“YOLOv8 경험을 화장품 리뷰 분석/제품 이미지 분석에 적용하려면 어떻게 확장할 수 있죠?”

“제품 사진에서 품질 검수/성분 라벨 인식/패키지 디자인 평가 같은 문제를 해결할 수 있을까요?”

“세그멘테이션 기반 도시 활력도 분석 경험을 활용해 화장품에서 어떤 피처를 추출할 수 있을까요?”

의도

→ 네 CV 경험이 추상적 연구인지, 실제 산업 문제 해결로 전이 가능한지 확인.

 2) “NLP 활용해 화장품 산업의 어떤 비효율을 해결한다고 보나요?”

너는 NLP 기본기(NER, 감성분석, 토픽모델링)를 했고
제로파티 데이터 기반 추천 시스템 경험도 있어.

예상 질문

“화장품 산업의 고객 리뷰나 전후 사진에서 어떤 인사이트를 NLP로 뽑을 수 있을까요?”

“감성 분석이나 Topic Modeling을 어떤 기준으로 적용할 수 있을까요?”

“MBTI·심리그래픽 기반 추천 경험을 B2C 뷰티 추천에 적용한다면 어떻게 전환되나요?”

“요즘 NLP에서 LLM 기반 RAG/분류/요약을 실제 제품 추천에 어떻게 연결할 수 있을까요?”

“성분 정보, 가격, 피부타입 등 structured data와 NLP 데이터를 어떻게 결합할 수 있을까요?”

의도

→ NLP를 단순 알고리즘 수준이 아니라 ‘비즈니스 인사이트’ 수준에서 연결할 수 있는지 확인.

 3) “제품 이미지 + 텍스트 + 메타데이터 통합 처리 경험?”

Layerminder 프로젝트가 바로 여기에 걸려.

예상 질문

“Stable Diffusion 기반 이미지 생성 경험을 실제 화장품 PDP 이미지 개선에 활용 가능할까요?”

“OpenAI API로 제품 카피/스토리 자동 생성한 경험이 화장품 마케팅에도 적용 가능할까요?”

“이미지와 텍스트를 결합한 멀티모달 분석을 해본 경험이 있나요?”

“FAISS/CLIP 경험을 활용해 화장품 유사도 검색을 만든다면 어떤 구조로 짜겠어요?”

의도

→ 네가 실제 상용 제품에 바로 투입 가능한 멀티모달 엔지니어인지 확인.

 4) “화장품 산업의 Pain Point를 알고 있나?”

기술만 아는 게 아니라 도메인 인사이트도 필요하다고 생각해 질문한다.

예상 질문

“화장품 산업에서 가장 비효율적인 부분이 뭐라고 보나요?”
예:
✔ 재고 관리 / SKU 폭증
✔ PDP 이미지·리뷰 관리
✔ 제품 기획 단계의 실패 비용
✔ 고객 리뷰 분석(QA, 결함, 피부 트러블 등)
✔ 마케팅 비용 비효율

“이 중에서 ML로 해결할 수 있는 문제는 무엇인가요?”

“기술의 한계는 무엇이라고 생각하나요?”

 5) “화장품 데이터의 난점 이해?”

엔지니어는 ‘문제 구조화 능력’을 봄.

예상 질문

“화장품 이미지는 조명·피부톤·카메라 품질에 따라 편향이 생기는데, 이를 어떻게 해결할 수 있을까요?”

“성분 데이터는 표기 방식이 달라 텍스트 클리닝이 어려운데, 어떻게 정규화하겠습니까?”

“리뷰의 감성은 계절/트렌드에 따라 bias가 생기는데, 이를 어떻게 다루나요?”

 6) “CV/NLP → 추천/검색 → 비즈니스 성과 연결 가능?”

너는 추천 시스템을 해봤으니까 이걸 연관하여 질문할 가능성 높음.

예상 질문

“제로파티 기반 추천 경험을 뷰티 도메인에 적용하면 어떤 아키텍처로 바뀔까요?”

“FAISS 기반 임베딩 검색으로 화장품 유사도 서비스를 만든다면 어떤 피처를 쓰겠어요?”

“사용자 프로필 + 이미지 분석 + 리뷰 NLP를 통합해서 개인화 추천을 만들 수 있을까요?”

 7) “LLM이 들어가면 어떤 MVP를 만들 자신 있나요?”

요즘 회사들은 이걸 물어본다.

예상 질문

“LLM을 이용해 화장품 산업에서 가장 빠르게 만들 수 있는 AI 기능은 무엇인가요?”

“LLM으로 피부타입 상담 Assistant를 만든다면 어떤 구조로 만들겠습니까?”

“카탈로그 자동 분류/요약/성분 위험도 설명을 자동화할 수 있을까요?”

 너한테 ‘특히 유리한 답변 방향’

너 이력서 기준으로 자연스럽게 이렇게 연결하면 됨:

도시 이미지 분석 → 제품 이미지 분석(세그멘테이션/구도/조명 등)

YOLOv8 추적 → QC·불량 검출 자동화

Stable Diffusion → PDP 이미지 자동 보정·생성

CLIP/FAISS → 화장품 이미지 유사도 기반 추천

NLP 경험 → 리뷰 요약·감성 추출·성분 리스크 설명

Layerminder → 실제 서비스화 경험을 AI 뷰티 제품에 재활용
"""