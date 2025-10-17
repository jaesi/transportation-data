INSERT INTO bv_platform_vision.tb_metropolitan_complex_workplace_od_202407
SELECT * FROM dblink(
              'host=10.10.12.181 dbname=dataops user=postgres password=i+;[bqPUlwpr',
              'SELECT * FROM "문재식".tb_metropolitan_complex_workplace_od_202407'
              ) AS t(
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

DROP TABLE bv_platform_vision.tb_metropolitan_complex_workplace_od_202407;
CREATE TABLE bv_platform_vision.tb_metropolitan_complex_workplace_od_202407
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

-- 1) 단일 컬럼 PK
ALTER TABLE bv_platform_vision.tb_metropolitan_complex_workplace_od_202407
ADD CONSTRAINT pk_metro_work_od
PRIMARY KEY (complex_key, work_station_id, work_station_type, work_station_grid_id);

-- 공간 인덱스 설정
CREATE INDEX idx_complex_geometry
ON bv_platform_vision.tb_metropolitan_complex_workplace_od_202407
USING GIST (complex_geometry);

SELECT complex_name, work_station_name,
       complex_morning_daily_commute_count,
       complex_household_count,
       complex_household_cluster_ratio,
       complex_morning_commute_average_time,
       complex_morning_commute_median_time
       FROM bv_platform_vision.tb_metropolitan_complex_workplace_od_202407
WHERE complex_name = '하안주공4단지'
ORDER BY complex_morning_daily_commute_count DESC;


