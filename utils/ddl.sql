---------------------------------------
---- 1. DB 스키마 생성: 통근OD 데이터 ----
---------------------------------------
DROP TABLE IF EXISTS 문재식.tb_metropolitan_work_od CASCADE;
CREATE TABLE 문재식.tb_metropolitan_work_od (
    standard_ym                     VARCHAR(6)  NOT NULL
    , departure_station_id          VARCHAR(20) NOT NULL
    , departure_station_name        VARCHAR(100)
    , departure_region_code         VARCHAR(20) NOT NULL
    , departure_station_type        VARCHAR(1)  NOT NULL
    , departure_grid_id             VARCHAR(12)
    , arrival_station_id            VARCHAR(20) NOT NULL
    , arrival_station_name          VARCHAR(100)
    , arrival_region_code           VARCHAR(20) NOT NULL
    , arrival_station_type          VARCHAR(1)  NOT NULL
    , arrival_grid_id               VARCHAR(12)
    , purpose_name                  VARCHAR(2)  NOT NULL
    , daily_use_count               NUMERIC(10,1)
    , median_elapse_time            NUMERIC(6,0)
    , average_elapse_time           NUMERIC(6,0)
    , median_distance               NUMERIC(6,0)
    , average_distance              NUMERIC(6,0)
    , departure_station_geometry    GEOMETRY(POINT, 5179)
    , arrival_station_geometry      GEOMETRY(POINT, 5179)
    , CONSTRAINT tb_metropolitan_work_od_pk
      PRIMARY KEY (standard_ym
                  , departure_station_id
                  , departure_region_code
                  , departure_station_type
                  , arrival_station_id
                  , arrival_region_code
                  , arrival_station_type
                  , purpose_name
                  )
)
PARTITION BY LIST (standard_ym);


-- 2. 자식 테이블 생성
CREATE TABLE 문재식.tb_metropolitan_work_od_202501
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202501');
CREATE TABLE 문재식.tb_metropolitan_work_od_202502
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202502');
CREATE TABLE 문재식.tb_metropolitan_work_od_202503
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202503');
CREATE TABLE 문재식.tb_metropolitan_work_od_202504
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202504');
CREATE TABLE 문재식.tb_metropolitan_work_od_202505
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202505');
CREATE TABLE 문재식.tb_metropolitan_work_od_202506
PARTITION OF 문재식.tb_metropolitan_work_od
FOR VALUES IN ('202506');

-- 3. GRID_ID -> 한글로 변환
-- departure_grid_id의 앞 두 글자에 대한 함수 기반 인덱스 생성
CREATE INDEX idx_departure_grid_id_prefix ON 문재식.tb_metropolitan_work_od (LEFT(departure_grid_id, 2));
DROP INDEX IF EXISTS idx_departure_grid_id_prefix;

-- arrival_grid_id의 앞 두 글자에 대한 함수 기반 인덱스 생성
CREATE INDEX idx_arrival_grid_id_prefix ON 문재식.tb_metropolitan_work_od (LEFT(arrival_grid_id, 2));
DROP INDEX IF EXISTS idx_arrival_grid_id_prefix;

-- 출발 정류장 그리드 업데이트
UPDATE 문재식.tb_metropolitan_work_od AS od
SET departure_grid_id = g.hangul || SUBSTRING(od.departure_grid_id, 3) -- 앞 두 글자를 변환하고 나머지 부분을 붙임
FROM 배지용.tb_grid_alphabet_to_hangul AS g
WHERE LEFT(od.departure_grid_id, 2) = g.alphabet
  AND od.departure_grid_id IS NOT NULL; -- NULL 값은 업데이트하지 않음

-- 도착 정류장 그리드 업데이트
UPDATE 문재식.tb_metropolitan_work_od AS od
SET arrival_grid_id = g.hangul || SUBSTRING(od.arrival_grid_id, 3)
FROM 배지용.tb_grid_alphabet_to_hangul AS g
WHERE LEFT(od.arrival_grid_id, 2) = g.alphabet
  AND od.arrival_grid_id IS NOT NULL;
-----------------------------------
---- 2. 일별/시간대별 집계 데이터 ----
-----------------------------------

DROP TABLE IF EXISTS 문재식.tb_metropolitan_daily_od CASCADE;
CREATE TABLE 문재식.tb_metropolitan_daily_od (
    standard_date                   DATE        NOT NULL
    , hour                          VARCHAR(2)  NOT NULL
    , departure_station_id          VARCHAR(20) NOT NULL
    , departure_station_name        VARCHAR(100)
    , departure_region_code         VARCHAR(20) NOT NULL
    , departure_station_type        VARCHAR(2)  NOT NULL
    , departure_grid_id             VARCHAR(12)
    , arrival_station_id            VARCHAR(20) NOT NULL
    , arrival_station_name          VARCHAR(100)
    , arrival_region_code           VARCHAR(20) NOT NULL
    , arrival_station_type          VARCHAR(2)  NOT NULL
    , arrival_grid_id               VARCHAR(12)
    , trip_type                     VARCHAR(2)  NOT NULL
    , use_count                     NUMERIC(10,0)
    , unique_use_count              NUMERIC(10,0)
    , median_elapse_time            NUMERIC(6,0)
    , average_elapse_time           NUMERIC(6,0)
    , median_distance               NUMERIC(6,0)
    , average_distance              NUMERIC(6,0)
    , median_transfer_count         NUMERIC(6,1)
    , average_transfer_count        NUMERIC(6,1)
    , departure_station_geometry    GEOMETRY(POINT, 5179)
    , arrival_station_geometry      GEOMETRY(POINT, 5179)
    , CONSTRAINT tb_metropolitan_daily_od_pk
      PRIMARY KEY (standard_date
                  , hour
                  , departure_station_id
                  , departure_region_code
                  , departure_station_type
                  , arrival_station_id
                  , arrival_region_code
                  , arrival_station_type
                  , trip_type
                  )
)
PARTITION BY RANGE (standard_date);


-- 2. 자식 테이블 생성
CREATE TABLE 문재식.tb_metropolitan_daily_od_202407
PARTITION OF 문재식.tb_metropolitan_daily_od
FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');


