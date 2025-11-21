/*
  본 프로세스는 출퇴근OD 데이터를 활용하여 연구용 단지 단위 업무 접근성 데이터를 산출하는 과정
  1. 단지형 아파트 + 보로노이 클러스터 매핑 테이블 생성
  2. 단지형 아파트 + 업무지 정류장 OD 데이터 머티리얼라이즈 테이블 생성
*/

---------- DDL ----------
-- 1. 단지형 아파트 보로노이 지오메트리 테이블 생성
DROP TABLE 문재식.tb_metropolitan_danji_voronoi_cluster;
-- 1.1. 단지형 아파트만 고려
CREATE UNLOGGED TABLE 문재식.temporary_metropolitan_danji_voronoi_cluster_only_danji AS
-- 서브쿼리
SELECT *
FROM (SELECT a.complex_key,
      a.complex_name,
      a.legaldong_code,
      a.road_name_address_management_number,
      a.road_name_address,
      a.complex_bv_house_division_name,
      a.complex_household_count,
      -- 동일 클러스터 내 세대수 비중
      a.complex_household_count / SUM(complex_household_count) OVER (PARTITION BY c.cluster_id) AS complex_household_ratio,
      b.geometry,
      c.cluster_id::INT
       FROM bv_platform_data_mart.tb_bv_platform_building_complex a
            -- 1.1. 단지 GEOMETRY 붙이기
            LEFT JOIN centroid.tb_road_name_address_building_geometry_202505 b
                ON a.road_name_address_management_number = b.road_name_address_management_number
            -- 1.2. 지하철역 기준 보로노이 폴리곤 불러오기 - 공간 조인
            JOIN 문재식.tb_metropolitan_voronoi_cluster c
                ON ST_Intersects(ST_Centroid(b.geometry), c.geometry)
       WHERE (a.legaldong_code LIKE '11%' OR a.legaldong_code LIKE '41%' OR a.legaldong_code LIKE '28%')
            AND complex_bv_house_division_name = '단지형아파트'); -- NULL값을 제외한 주거용 건물 모두 고려

-- 1.2. 다른 주거 유형 고려
CREATE TABLE 문재식.tb_metropolitan_danji_voronoi_cluster AS
SELECT *
FROM (SELECT a.complex_key,
      a.complex_name,
      a.legaldong_code,
      a.road_name_address_management_number,
      a.road_name_address,
      a.complex_bv_house_division_name,
      a.complex_household_count,
      -- 동일 클러스터 내 세대수 비중
      a.complex_household_count / SUM(complex_household_count) OVER (PARTITION BY c.cluster_id) AS complex_household_ratio,
      b.geometry,
      c.cluster_id::INT
       FROM bv_platform_data_mart.tb_bv_platform_building_complex a
            -- 1.1. 단지 GEOMETRY 붙이기
            LEFT JOIN centroid.tb_road_name_address_building_geometry_202505 b
                ON a.road_name_address_management_number = b.road_name_address_management_number
            -- 1.2. 지하철역 기준 보로노이 폴리곤 불러오기 - 공간 조인
            JOIN 문재식.tb_metropolitan_voronoi_cluster c
                ON ST_Intersects(ST_Centroid(b.geometry), c.geometry)
       WHERE (a.legaldong_code LIKE '11%' OR a.legaldong_code LIKE '41%' OR a.legaldong_code LIKE '28%')
            AND complex_bv_house_division_name IS NOT NULL) -- NULL값을 제외한 주거용 건물 모두 고려
WHERE complex_bv_house_division_name = '단지형아파트';


-- 2) PK 추가
ALTER TABLE 문재식.tb_metropolitan_danji_voronoi_cluster
ADD CONSTRAINT tb_metropolitan_danji_voronoi_cluster_pkey
PRIMARY KEY (complex_key);

CREATE INDEX idx_voronoi_geom ON 문재식.tb_metropolitan_danji_voronoi_cluster
USING GIST (geometry);

-- 2. 서울, 경기 단지형아파트 업무 접근성 데이터
DROP TABLE 문재식.tb_metropolitan_complex_workplace_od_202407;
CREATE TABLE 문재식.tb_metropolitan_complex_workplace_od_202407
    (
        complex_key VARCHAR(9),
        complex_name VARCHAR(500),
        road_name_address_management_number VARCHAR(26),
        road_name_address VARCHAR(500),
        complex_cluster_id VARCHAR(5),
        complex_cluster_name VARCHAR(100),
        work_station_id VARCHAR(20),
        work_station_name VARCHAR(50),
        work_station_type VARCHAR(1),
        work_station_cluster_id VARCHAR(5),
        work_station_grid_id VARCHAR(12),
        business_district_name VARCHAR(50),
        complex_household_count NUMERIC,
        complex_household_cluster_ratio NUMERIC,
        complex_morning_daily_commute_count NUMERIC,
        complex_morning_commute_average_time NUMERIC,
        complex_morning_commute_median_time NUMERIC,
        complex_morning_commute_average_distance NUMERIC,
        complex_morning_commute_median_distance NUMERIC,
        complex_geometry GEOMETRY(GEOMETRY, 5179),
        work_station_geometry GEOMETRY(POINT, 5179)
    );


-- 인덱스 추가
ALTER TABLE 문재식.tb_metropolitan_complex_workplace_od_202407
ADD CONSTRAINT pk_complex_cluster_id PRIMARY KEY (complex_key, work_station_id, work_station_type, work_station_grid_id)

-- 공간 인덱스 설정
CREATE INDEX idx_complex_geometry
ON 문재식.tb_metropolitan_complex_workplace_od_202407
USING GIST (complex_geometry);

DROP INDEX IF EXISTS 문재식.tb_metropolitan_complex_workplace_od_202407.idx_complex_geometry;

SELECT indexname
FROM pg_indexes
WHERE schemaname = '문재식'
  AND tablename  = 'tb_metropolitan_complex_workplace_od_202407';

-- 단지 테이블 centroid 추가해두기
ALTER TABLE 문재식.tb_metropolitan_danji_voronoi_cluster
ADD COLUMN centroid_geometry geometry(POINT, 5179);
UPDATE 문재식.tb_metropolitan_danji_voronoi_cluster
SET centroid_geometry = ST_Centroid(geometry);

-- 3. 임시 테이블 생성
DROP TABLE 문재식.temporary_metropolitan_danji_workplace_od_top10_202407_station;
CREATE UNLOGGED TABLE 문재식.temporary_metropolitan_danji_workplace_od_top10_202407_station
    (
        complex_key VARCHAR(9),
        complex_name VARCHAR(500),
        road_name_address_management_number VARCHAR(26),
        road_name_address VARCHAR(500),
        complex_bv_house_division_name VARCHAR(10),
        danji_cluster_id VARCHAR(5),
        danji_cluster_name VARCHAR(100),
        work_station_id VARCHAR(30),
        work_station_name VARCHAR(50),
        work_station_type VARCHAR(1),
        work_station_grid_id VARCHAR(12),
        complex_household_count NUMERIC,
        complex_household_cluster_ratio FLOAT,
        danji_morning_daily_commute_count FLOAT,
        danji_morning_commute_average_time FLOAT,
        danji_morning_commute_median_time FLOAT,
        danji_morning_commute_average_distance FLOAT,
        danji_morning_commute_median_distance FLOAT,
        danji_geometry GEOMETRY(GEOMETRY, 5179),
        work_station_geometry GEOMETRY(GEOMETRY, 5179),
        PRIMARY KEY (complex_key, work_station_id)
    );
-- 공간 인덱스 설정
CREATE INDEX idx_danji_geometry
ON 문재식.tb_seoul_gyeonggi_danji_workplace_od_202407
USING GIST (danji_geometry);


-- 4. 정류장 distinct 임시 테이블 만들기
DROP TABLE 문재식.temporary_metropolitan_station_geometry;
CREATE UNLOGGED TABLE 문재식.temporary_metropolitan_station_geometry AS(
WITH station_union AS (
    SELECT live_station_id AS station_id,
           live_station_name AS station_name,
           live_station_legaldong AS station_legaldong,
           live_station_grid_id AS station_grid_id,
           live_station_type AS station_type,
           live_station_geometry AS geometry
        FROM 문재식.tb_metropolitan_morning_commute_od_202407
    UNION ALL
    SELECT work_station_id AS station_id,
           work_station_name AS station_name,
           work_station_legaldong AS station_legaldong,
           work_station_grid_id AS station_grid_id,
           work_station_type AS station_type,
           work_station_geometry AS geometry
        FROM 문재식.tb_metropolitan_morning_commute_od_202407
        )
SELECT DISTINCT station_id, station_name, station_legaldong, station_grid_id, station_type,
                 geometry
FROM station_union);

-- PK 제약 추가
ALTER TABLE 문재식.temporary_metropolitan_station_geometry
ADD CONSTRAINT pk_temp_station
PRIMARY KEY (station_id, station_grid_id, station_type);

-- 공간 인덱스 추가
CREATE INDEX idx_station_geometry
ON 문재식.temporary_metropolitan_station_geometry
USING GIST (geometry);
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-- 신당삼성 아파트 예시 -> 단지 키로 하나로 만들어버림
-- pnu는 1114016200108430000와 1114016200108430001로 두 개이나 하나의 건물명으로 묶임
SELECT agg_household_count, title_household_count, dong_name
    --agg_household_count, title_household_count, ppk, jpk, pnu, dong_name, ho_name
FROM building_document.tb_building_total_202506
WHERE building_name = '신당동삼성아파트'
group by agg_household_count, title_household_count, dong_name
order by dong_name;

-- pnu를 1114016200108430000 하나로 만들어버림
SELECT complex_household_count, complex_ho_count, complex_dong_count
FROM bv_platform_data_mart.tb_bv_platform_building_complex
WHERE complex_name = '신당삼성';


--------------------------------------------------------------------------------------
-- 프로세스 정리:
-- 1. tb_metropolitan_voronoi_cluster 산출 -> 지하철역 환승역 정제
-- 2. tb_metropolitan_danji_voronoi_cluster 산출 : 단지 + 보로노이 클러스터 + 세대수 비중 컬럼 추가

-- 3. 기존 통근OD 테이블 -> 클러스터 단위 OD 머티리얼라이즈 테이블 구축
DROP MATERIALIZED VIEW  문재식.mv_od_commute_summary;
CREATE MATERIALIZED VIEW 문재식.mv_od_commute_summary AS
SELECT
c1.cluster_id   AS live_station_cluster_id,
c1.station_name AS live_station_cluster_name,
a.work_station_id,
a.work_station_name,
a.work_station_type,
a.work_station_grid_id,
a.work_station_geometry,
c2.cluster_id AS work_station_cluster_id,
SUM(a.morning_daily_commute_count)    AS commute_sum,
AVG(a.morning_commute_average_time)   AS avg_time,
AVG(a.morning_commute_median_time)    AS median_time,
AVG(a.morning_commute_average_distance) AS avg_dist,
AVG(a.morning_commute_median_distance)  AS median_dist
FROM 문재식.tb_metropolitan_morning_commute_od_202407 a
JOIN 문재식.tb_metropolitan_voronoi_cluster c1
ON a.live_station_geometry && c1.geometry
AND ST_Intersects(a.live_station_geometry, c1.geometry)
JOIN 문재식.tb_metropolitan_voronoi_cluster c2
ON a.work_station_geometry && c2.geometry
AND ST_Intersects(a.work_station_geometry, c2.geometry)
GROUP BY c1.cluster_id, a.work_station_id, a.work_station_name,
       a.work_station_type, a.work_station_grid_id, a.work_station_geometry, c2.cluster_id;


-- 1. 수도권(서울, 경기, 인천) 단지형 아파트만 추출 - Lateral Join 활용하여 상위 10개만 추출하여 조인 적용
INSERT INTO 문재식.temporary_metropolitan_danji_workplace_od_top10_202407_danji;

-- 1.1. 단지형 아파트만 고려
SELECT complex_name, road_name_address, work_station_name, work_station_type, complex_household_count,
       complex_household_cluster_ratio, danji_morning_daily_commute_count
FROM (SELECT d.complex_key,
       d.complex_name,
       d.road_name_address_management_number,
       d.road_name_address,
       d.complex_bv_house_division_name,
       d.cluster_id AS danji_cluster_id,
       o.live_station_cluster_name AS danji_cluster_name,
       o.work_station_id,
       o.work_station_name,
       o.work_station_type,
       o.work_station_grid_id,
       d.complex_household_count,
       ROUND(d.complex_household_ratio, 2) AS complex_household_cluster_ratio,
       ROUND(d.complex_household_ratio * o.commute_sum, 1) AS danji_morning_daily_commute_count,
       ROUND(o.avg_time, 1) AS danji_morning_commute_average_time,
       ROUND(o.median_time, 1) AS danji_morning_commute_median_time,
       ROUND(o.avg_dist, 1) AS danji_morning_commute_average_distance,
       ROUND(o.median_dist, 1) AS danji_morning_commute_median_distance,
       d.geometry AS danji_geometry,
       o.work_station_geometry
FROM 문재식.temporary_metropolitan_danji_voronoi_cluster_only_danji d -- 단지형아파트 + 보로노이 클러스터
JOIN LATERAL (
    SELECT *
        FROM 문재식.mv_od_commute_summary o -- 주거클러스터id -> 업무지 정류장 OD 데이터
    WHERE d.cluster_id = o.live_station_cluster_id::INT
    AND (d.complex_household_ratio * o.commute_sum) > 0
    ORDER BY o.commute_sum DESC
    LIMIT 10) AS o
ON TRUE);


-- 1.2. 모든 주거 유형 고려
INSERT INTO 문재식.tb_metropolitan_complex_workplace_od_202407
SELECT
  d.complex_key,
  d.complex_name,
  d.road_name_address_management_number,
  d.road_name_address,
  d.cluster_id             AS complex_cluster_id,
  o.live_station_cluster_name AS complex_cluster_name,
  o.work_station_id,
  o.work_station_name,
  o.work_station_type,
  o.work_station_cluster_id,
  o.work_station_grid_id,
  CASE
      WHEN
      o.work_station_cluster_id IN ('235', '236', '300', '308', '291', '305', '312', '321', '301', '304', '310', '324')
      THEN '종로'
      WHEN o.work_station_cluster_id IN ('373', '378', '400', '438', '454') THEN '강남'
      WHEN o.work_station_cluster_id IN ('365', '385') THEN '논현동'
      WHEN o.work_station_cluster_id = '352' THEN '교대'
      WHEN o.work_station_cluster_id = '382' THEN '압구정'
      WHEN o.work_station_cluster_id = '370' THEN '양재'
      WHEN o.work_station_cluster_id = '533' THEN '잠실'
      WHEN o.work_station_cluster_id = '559' THEN '문정'
      WHEN o.work_station_cluster_id IN ('407', '547', '555') THEN '분당'
      WHEN o.work_station_cluster_id = '458' THEN '노원'
      WHEN o.work_station_cluster_id = '170' THEN '발산'
      WHEN o.work_station_cluster_id = '211' THEN '영등포'
      WHEN o.work_station_cluster_id IN ('230', '251') THEN '여의도'
      WHEN o.work_station_cluster_id = '289' THEN '용산'
      WHEN o.work_station_cluster_id = '193' THEN '가산디지털단지'
      WHEN o.work_station_cluster_id IN ('199', '208') THEN '구로'
      WHEN o.work_station_cluster_id IN ('323', '347') THEN '동대문'
      WHEN o.work_station_cluster_id = '447' THEN '성수'
      WHEN o.work_station_cluster_id = '34' THEN '인천 테크노파크'
      WHEN o.work_station_cluster_id IN ('315', '363') THEN '수원'
      WHEN o.work_station_cluster_id = '267' THEN '범계'
      END AS business_district_name,
  d.complex_household_count,
  ROUND(d.complex_household_ratio::numeric, 2) AS complex_household_cluster_ratio,
  ROUND((d.complex_household_ratio::numeric * o.commute_sum)::numeric, 1)
    AS complex_morning_daily_commute_count,
  ROUND(o.avg_time::numeric, 1)    AS complex_morning_commute_average_time,
  ROUND(o.median_time::numeric, 1) AS complex_morning_commute_median_time,
  ROUND(o.avg_dist::numeric, 1)    AS complex_morning_commute_average_distance,
  ROUND(o.median_dist::numeric, 1) AS complex_morning_commute_median_distance,
  d.geometry               AS complex_geometry,
  o.work_station_geometry
FROM 문재식.tb_metropolitan_danji_voronoi_cluster d
CROSS JOIN LATERAL (
  SELECT
    o.live_station_cluster_name,
    o.work_station_id,
    o.work_station_name,
    o.work_station_type,
    o.work_station_grid_id,
    o.work_station_cluster_id,
    o.commute_sum,
    o.avg_time,
    o.median_time,
    o.avg_dist,
    o.median_dist,
    o.work_station_geometry
  FROM 문재식.mv_od_commute_summary o
  WHERE o.live_station_cluster_id::INT = d.cluster_id::INT
    AND o.commute_sum > 0
  ORDER BY o.commute_sum DESC
  LIMIT 10
) AS o;

SELECT * FROM 문재식.mv_od_commute_summary;

SELECT * FROM 문재식.tb_metropolitan_complex_workplace_od_202407
WHERE complex_cluster_name = '독산'
ORDER BY complex_key, complex_morning_daily_commute_count desc;

SELECT d.complex_key,
       d.complex_name,
       d.road_name_address_management_number,
       d.road_name_address,
       d.cluster_id AS copmlex_cluster_id,
       o.live_station_cluster_name AS copmlex_cluster_name,
       o.work_station_id,
       o.work_station_name,
       o.work_station_type,
       o.work_station_grid_id,
       d.complex_household_count,
       ROUND(d.complex_household_ratio, 2) AS complex_household_cluster_ratio,
       ROUND(d.complex_household_ratio * o.commute_sum, 1) AS complex_morning_daily_commute_count,
       ROUND(o.avg_time, 1) AS complex_morning_commute_average_time,
       ROUND(o.median_time, 1) AS complex_morning_commute_median_time,
       ROUND(o.avg_dist, 1) AS complex_morning_commute_average_distance,
       ROUND(o.median_dist, 1) AS complex_morning_commute_median_distance,
       d.geometry AS complex_geometry,
       o.work_station_geometry
FROM 문재식.tb_metropolitan_danji_voronoi_cluster d -- 단지형아파트 + 보로노이 클러스터
JOIN LATERAL (
    SELECT *
        FROM 문재식.mv_od_commute_summary o -- 주거클러스터id -> 업무지 정류장 OD 데이터
    WHERE d.cluster_id = o.live_station_cluster_id::INT
    AND (d.complex_household_ratio * o.commute_sum) > 0
    ORDER BY o.commute_sum DESC
    LIMIT 10) AS o
ON TRUE;


SELECT
  d.complex_key,
  d.complex_name,
  d.road_name_address_management_number,
  d.road_name_address,
  d.cluster_id             AS complex_cluster_id,
  o.live_station_cluster_name AS complex_cluster_name,
  o.work_station_id,
  o.work_station_name,
  o.work_station_type,
  o.work_station_grid_id,
  d.complex_household_count,
  ROUND(d.complex_household_ratio::numeric, 2) AS complex_household_cluster_ratio,
  ROUND((d.complex_household_ratio::numeric * o.commute_sum)::numeric, 1)
    AS complex_morning_daily_commute_count,
  ROUND(o.avg_time::numeric, 1)    AS complex_morning_commute_average_time,
  ROUND(o.median_time::numeric, 1) AS complex_morning_commute_median_time,
  ROUND(o.avg_dist::numeric, 1)    AS complex_morning_commute_average_distance,
  ROUND(o.median_dist::numeric, 1) AS complex_morning_commute_median_distance,
  d.geometry               AS complex_geometry,
  o.work_station_geometry
FROM 문재식.tb_metropolitan_danji_voronoi_cluster d
CROSS JOIN LATERAL (
  SELECT
    o.live_station_cluster_name,
    o.work_station_id,
    o.work_station_name,
    o.work_station_type,
    o.work_station_grid_id,
    o.commute_sum,
    o.avg_time,
    o.median_time,
    o.avg_dist,
    o.median_dist,
    o.work_station_geometry
  FROM 문재식.mv_od_commute_summary o
  WHERE o.live_station_cluster_id::INT = d.cluster_id::INT
    AND o.commute_sum > 0
  ORDER BY o.commute_sum DESC
  LIMIT 10
) AS o;




WITH od_ranked AS (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY live_station_id::INT, live_station_type, live_station_grid_id
        ORDER BY morning_daily_commute_count DESC
        ) AS rn
    FROM 문재식.tb_metropolitan_morning_commute_od_202407
    WHERE morning_daily_commute_count > 0
),
WITH cluster_linked AS(
    SELECT r.*,
           v.cluster_id,
           v.station_name AS cluster_station_name
    FROM 문재식.tb_metropolitan_morning_commute_od_202407 r
    JOIN 문재식.tb_metropolitan_voronoi_cluster v
    ON ST_Intersects(r.live_station_geometry, v.geometry)
),
aggregated AS(
    SELECT
    dvc.complex_key,
    MAX(dvc.complex_name) AS complex_name,
    MAX(dvc.road_name_address_management_number) AS road_name_address_management_number,
    MAX(dvc.road_name_address) AS road_name_address,
    MAX(dvc.cluster_id) AS complex_cluster_id,
    cl.work_station_id,
    MAX(cl.work_station_name) AS work_station_name,
    cl.work_station_type,
    cl.work_station_grid_id,
    MAX(dvc.complex_household_count) AS complex_household_count,
    MAX(dvc.complex_household_ratio) AS complex_household_ratio,
    SUM(cl.morning_daily_commute_count) AS complex_morning_daily_commute_count,
    AVG(cl.morning_commute_average_time) AS morning_commute_average_time,
    AVG(cl.morning_commute_median_time) AS morning_commute_median_time,
    AVG(cl.morning_commute_average_distance) AS morning_commute_average_distance,
    AVG(cl.morning_commute_median_distance) AS morning_commute_median_distance,
    MAX(dvc.geometry) AS complex_geometry,
    MAX(cl.work_station_geometry) AS work_station_geometry
    FROM cluster_linked cl
    JOIN 문재식.tb_metropolitan_danji_voronoi_cluster dvc -- 단지 + 보로노이 클러스터
        ON cl.cluster_id::INT = dvc.cluster_id
    GROUP BY dvc.complex_key, cl.work_station_id, cl.work_station_grid_id, cl.work_station_type
),
-- 단지별 top-10 필터
ranked AS (
    SELECT
    *,
    ROW_NUMBER() OVER(
    PARTITION BY complex_key
    ORDER BY complex_morning_daily_commute_count DESC
    ) AS rank
    FROM aggregated
)
SELECT
  complex_key,
  complex_name,
  road_name_address_management_number,
  road_name_address,
  complex_cluster_id,
  work_station_id,
  work_station_type,
  work_station_grid_id,
  complex_household_count,
  complex_household_ratio,
  complex_morning_daily_commute_count,
  morning_commute_average_time,
  morning_commute_median_time,
  morning_commute_average_distance,
  morning_commute_median_distance,
  complex_geometry,
  work_station_geometry
FROM ranked
WHERE rank <= 10
ORDER BY complex_key, complex_morning_daily_commute_count DESC;



-- 권역으로 집계 로직
-- 상위 20개 확인
SELECT MAX(work_station_name) AS work_station_name,
       ROUND(SUM(complex_morning_daily_commute_count)::numeric, 1) AS daily_commute_count,
       MAX(work_station_geometry) AS work_station_geometry
FROM 문재식.tb_metropolitan_complex_workplace_od_202407
GROUP BY work_station_id, work_station_type, work_station_grid_id
ORDER BY SUM(complex_morning_daily_commute_count) DESC
LIMIT 20;

--
WITH alight_percentile AS (SELECT MAX(work_station_name)                                      AS work_station_name,
                                  ROUND(SUM(complex_morning_daily_commute_count)::numeric, 1) AS daily_commute_sum,
                                  MAX(work_station_geometry)                                  AS work_station_geometry
                           FROM 문재식.tb_metropolitan_complex_workplace_od_202407
                           GROUP BY work_station_id, work_station_type, work_station_grid_id
                           ORDER BY SUM(complex_morning_daily_commute_count) DESC)
SELECT *
FROM alight_percentile
WHERE daily_commute_sum >= (
    SELECT PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY daily_commute_sum)
    FROM alight_percentile
    );

WITH
-- 1) T 타입 정류장 중 이름이 같은 것들 대표 ID 매핑
station_canonical AS (
  SELECT
    work_station_name,
    MIN(work_station_id) AS canonical_id
  FROM 문재식.tb_metropolitan_complex_workplace_od_202407
  WHERE work_station_type = 'T'
  GROUP BY work_station_name
  HAVING COUNT(DISTINCT work_station_id) > 1
),

-- 2) 이름 통합(unified) → 단일 station_id 로 집계
alight_percentile AS (
  SELECT
    -- T 타입이면 대표 ID, 아니면 원래 ID
    COALESCE(sc.canonical_id, d.work_station_id) AS unified_station_id,
    d.work_station_name,
    d.work_station_type,
    ROUND(SUM(d.complex_morning_daily_commute_count)::numeric, 1) AS daily_commute_sum,
    MAX(d.work_station_geometry)                       AS work_station_geometry
  FROM 문재식.tb_metropolitan_complex_workplace_od_202407 d
  LEFT JOIN station_canonical sc
    ON d.work_station_type = 'T'
   AND d.work_station_name = sc.work_station_name
  GROUP BY
    unified_station_id,
    d.work_station_name,
    d.work_station_type
)

-- 3) 상위 20% (80%) 필터
SELECT *
FROM alight_percentile ap
WHERE ap.daily_commute_sum >= (
  SELECT
    PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY daily_commute_sum)
  FROM alight_percentile
)
ORDER BY ap.daily_commute_sum DESC;

-- 권역 추가하기
SELECT * FROM 문재식.tb_metropolitan_complex_workplace_od_202407;

ALTER TABLE 문재식


-- 1. 통근 테이블 집계 - 상위 10개만 남기기
-- 1) distinct live_station_id 리스트
WITH stations AS (
  SELECT DISTINCT live_station_id
  FROM 문재식.tb_metropolitan_morning_commute_od_202407
);

-- 기존 개수: 3,473,898행
SELECT COUNT(*) FROM 문재식.tb_metropolitan_morning_commute_od_202407;



-- 거리 가중치 고려 버젼
WITH row_rank AS(
    SELECT *,
       ROW_NUMBER() OVER(
           PARTITION BY live_station_id
           ORDER BY morning_daily_commute_count DESC
           ) AS commute_rank
    FROM 문재식.tb_metropolitan_morning_commute_od_202407
),
od_top10 AS (
    SELECT *
    FROM row_rank
    WHERE commute_rank <=10
    ),
od_top10_voronoi AS (
    SELECT od.*
       , v.cluster_id as live_station_cluster_id
       , v.station_name as live_station_cluster_name
    FROM od_top10 od
    JOIN 문재식.tb_metropolitan_voronoi_cluster v
    ON ST_Intersects(od.live_station_geometry, v.geometry)
    ),
distance_household_weight AS (
    SELECT *,
           -- 거리x세대수 가중치
        SQRT(
          dv.complex_household_ratio
          * EXP(-0.001 * ST_Distance(dv.cengeometry, tv.live_station_geometry))
        ) AS dist_household_w
    FROM od_top10_voronoi tv
    JOIN 문재식.tb_metropolitan_danji_voronoi_cluster dv
        ON tv.live_station_cluster_id::INT = dv.cluster_id
),
normalized AS (
    SELECT *,
        dist_household_w
        / SUM(dist_household_w)
           OVER (PARTITION BY live_station_id, live_station_type, live_station_grid_id)
            AS normalized_weight
    FROM distance_household_weight
)
SELECT *
FROM normalized
;

SELECT business_district_name,
       ROUND(SUM(complex_morning_daily_commute_count)::INT,0) AS sum_commute
               FROM 문재식.tb_metropolitan_complex_workplace_od_202407
WHERE business_district_name IS NOT NULL
               GROUP BY business_district_name
ORDER BY SUM(complex_morning_daily_commute_count) DESC;