# 교통카드 통근OD 데이터 후처리 DAG
- 해당 dag프로세스는 데이터 안심구역에서 반출 후 S3에 업로드된 원본 통근OD Parquet 파일이 있어야 정상 작동
- 파일경로: ```교통카드/통근OD/{YYYYMM}/{YYYYMM}_workod_purpose_transport_grid.parquet```

## 1. 파라미터 설정
### 1) S3 버킷
- S3 버킷: bv-dropbox

### 2) 업로드 대상 DB 정보
- 업로드 DB: Postgres (dataops_test_181)                    *<< -- 현재는 테스트용으로 실제 운영 DB로 변경 필요*
- 스키마: temporary
- 테이블: tb_metropolitan_work_od (파티셔닝: standard_ym)

### 3) 대상 월
- Airflow Variable: transportation_target_months 혹은 DAG Run Config 의 transportation_target_months 파라미터 활용
- YYYYMM 형식의 문자열 리스트
- 미지정 시 기본값: 202506

## 2. Task 설명
### 1. [load_parquet_from_s3]
- S3에서 Parquet 파일 로드
- 경로 반환

### 2. [get_korean_business_day_task]
- 일평균 통행량을 위해 해당 달의 영업일 산출 
- holidays 라이브러리 활용하기 위해 VirtualEnvironmentOperator 활용

### 3. [transform_parquet]
- Parquet -> DataFrame
- 승차/하차 그리드 ID -> EPSG:5179 Point WKT
- 영업일 수 계산 (한국 휴일 반영)
- 일평균 이용건수 계산
- 목적명칭 매핑 (morning->출근, evening->퇴근)
- 영문 그리드 ID -> 국가표준 한글 그리드 ID 변환
- 컬럼명 영문 표준화
- CSV 문자열 반환

### 4. [load_to_postgres]
- PostgresHook 이용
- 테이블 및 파티션 자동 생성
- COPY FROM STDIN 으로 한 번에 적재