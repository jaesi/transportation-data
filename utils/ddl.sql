-- 1. DB 스키마 생성

DROP TABLE IF EXISTS 문재식.tb_metropolitan_work_od;
CREATE TABLE 문재식.tb_metropolitan_work_od (
    standard_ym VARCHAR(6)          NOT NULL
    , departure_station_id          VARCHAR(20)
    , departure_station_name        VARCHAR(100)
    , departure_region_code         VARCHAR(20)
    , departure_station_type        VARCHAR(1)
    , departure_grid_id             VARCHAR(12)
    , arrival_station_id            VARCHAR(20)
    , arrival_station_name          VARCHAR(100)
    , arrival_region_code           VARCHAR(20)
    , arrival_station_type          VARCHAR(1)
    , arrival_grid_id               VARCHAR(12)
    , purpose_name                  VARCHAR(2)
    , daily_use_count               NUMERIC(10,1)
    , median_elapse_time            NUMERIC(6,0)
    , average_elapse_time           NUMERIC(6,0)
    , median_distance               NUMERIC(6,0)
    , average_distance              NUMERIC(6,0)
    , departure_station_geometry    GEOMETRY(POINT, 5179)
    , arrival_station_geometry      GEOMETRY(POINT, 5179)
)
PARTITION BY LIST (standard_ym);
;

-- PK 설정
ALTER TABLE 문재식.tb_metropolitan_work_od
ADD CONSTRAINT tb_metropolitan_work_od_pk
PRIMARY KEY (standard_ym, departure_station_id, departure_region_code, departure_station_type,
             arrival_station_id, arrival_region_code, arrival_station_type, purpose_name);

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

