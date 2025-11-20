
-- DROP TABLE 문재식.tb_seoul_bus_stations;

-- Index(['정류장번호', '정류장명', '도시코드', '도시명', 'geometry'], dtype='object')
CREATE TABLE 문재식.tb_seoul_bus_stations
(node_id VARCHAR,
ars_id VARCHAR,
station_name VARCHAR,
geometry GEOMETRY(Point, 5179));

SELECT * FROM 문재식.tb_seoul_bus_stations;
SELECT  ST_AsText(geometry) FROM 문재식.tb_seoul_bus_stations;



-- Function: 5179 -> 10x10 GRID ID
CREATE OR REPLACE FUNCTION 문재식.get_10m_grid_id(x double precision, y double precision)
    RETURNS char(12) AS
$$
DECLARE
    ox_mapping char(1)[] := ARRAY ['A', 'B', 'C', 'D', 'E', 'F', 'G'];
    oy_mapping char(1)[] := ARRAY ['B', 'C', 'D', 'E', 'F', 'G', 'H'];
    ox_index   integer;
    oy_index   integer;
    grid_id    char(12);
    x_numeric numeric := x;
    y_numeric numeric := y;
BEGIN
    ox_index := floor(x_numeric / 100000) - 6;
    oy_index := floor(y_numeric / 100000) - 13;

    grid_id := ox_mapping[ox_index] ||
               oy_mapping[oy_index] ||
               right('000' || floor(x_numeric / 1), 5) ||
               right('000' || floor(y_numeric / 1), 5);

    RETURN grid_id;
EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 역산 함수
CREATE OR REPLACE FUNCTION 문재식.grid_id_to_xy(grid_id char(12))
    RETURNS TABLE (x double precision, y double precision) AS
$$
DECLARE
    ox_mapping char(1)[] := ARRAY ['A', 'B', 'C', 'D', 'E', 'F', 'G'];
    oy_mapping char(1)[] := ARRAY ['B', 'C', 'D', 'E', 'F', 'G', 'H'];
    ox_index integer;
    oy_index integer;
    x_str text;
    y_str text;
BEGIN
    ox_index := array_position(ox_mapping, substring(grid_id, 1, 1));
    oy_index := array_position(oy_mapping, substring(grid_id, 2, 1));

    IF ox_index IS NULL OR oy_index IS NULL THEN
        RETURN;
    END IF;

    x_str := substring(grid_id, 3, 5);
    y_str := substring(grid_id, 8, 5);

    x := ((ox_index + 6) * 100000) + (x_str::integer );
    y := ((oy_index + 13) * 100000) + (y_str::integer );

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- 함수 테스트
SELECT * FROM 문재식.get_10m_grid_id(1124661.62443, 1829906.742546843);

-- 역함수 테스트
SELECT * FROM 문재식.grid_id_to_xy('EF2466129906');

-- 버스정류장 매핑 테스트
with distinct_stations as (
SELECT DISTINCT ON (ST_AsText(geometry)) *
FROM 문재식.tb_seoul_bus_stations
)
SELECT
    문재식.get_10m_grid_id(ST_X(geometry), ST_Y(geometry)) AS grid_10m_id,

    COUNT(*) AS station_count
FROM distinct_stations
GROUP BY grid_10m_id
ORDER BY station_count DESC;

-- 버스정류장 매핑 테스트
with distinct_stations as (
SELECT DISTINCT ON (ST_AsText(geometry)) *
FROM 문재식.tb_seoul_bus_stations
)
SELECT
    문재식.get_10m_grid_id(ST_X(geometry), ST_Y(geometry)) AS grid_10m_id,
    *
FROM distinct_stations
WHERE 문재식.get_10m_grid_id(ST_X(geometry), ST_Y(geometry)) = 'CG5084649892';
operation_date VARCHAR(8) NOT NULL,
station_id VARCHAR(20) NOT NULL,
station_name VARCHAR(100),
station_type VARCHAR(1),
legaldong_code VARCHAR(10),
duration_type VARCHAR(5),
total_duration_time INT,
duration_count INT,
duration_average_time INT,
duration_median_time INT,
geometry GEOMETRY(Point, 5179)
);

-- 통근 OD 데이터를 위한 테이블 생성
DROP TABLE 문재식.tb_metropolitan_morning_commute_od_202407;
CREATE TABLE 문재식.tb_metropolitan_morning_commute_od_202407
(
live_station_id VARCHAR(20),
work_station_id VARCHAR(20),
 live_station_name VARCHAR(100),
 work_station_name VARCHAR(100),
 live_station_legaldong VARCHAR(10),
 work_station_legaldong VARCHAR(10),
live_station_grid_id VARCHAR(12),
work_station_grid_id VARCHAR(12),
live_station_type VARCHAR(1),
 work_station_type VARCHAR(1),
morning_daily_commute_count NUMERIC(10,1),
morning_commute_average_time INT,
 morning_commute_median_time INT,
morning_commute_average_distance INT,
 morning_commute_median_distance INT,
live_station_geometry GEOMETRY(Point, 5179),
work_station_geometry GEOMETRY(Point, 5179),
CONSTRAINT pk_morning_commute PRIMARY KEY (live_station_id, work_station_id, live_station_grid_id, work_station_grid_id,
    live_station_type, work_station_type)
);

DROP TABLE 문재식.tb_metropolitan_evening_commute_od_202407;
CREATE TABLE 문재식.tb_metropolitan_evening_commute_od_202407
(live_station_id VARCHAR(20),
work_station_id VARCHAR(20),
 live_station_name VARCHAR(100),
 work_station_name VARCHAR(100),
 live_station_legaldong VARCHAR(10),
 work_station_legaldong VARCHAR(10),
live_station_grid_id VARCHAR(12),
work_station_grid_id VARCHAR(12),
live_station_type VARCHAR(1),
 work_station_type VARCHAR(1),
evening_daily_commute_count NUMERIC(10,1),
evening_commute_average_time INT,
 evening_commute_median_time INT,
evening_commute_average_distance INT,
 evening_commute_median_distance INT,
live_station_geometry GEOMETRY(Point, 5179),
work_station_geometry GEOMETRY(Point, 5179),
CONSTRAINT pk_evening_commute PRIMARY KEY (live_station_id, work_station_id, live_station_grid_id, work_station_grid_id,
    live_station_type, work_station_type)
);

SELECT sum(morning_daily_commute_count) over() FROM 문재식.tb_metropolitan_morning_commute_od_202407
ORDER BY morning_daily_commute_count desc;

-- 공간 조인된 서울 테이블 생성
CREATE TABLE 문재식.tb_seoul_bus_station_legaldong
(node_id VARCHAR,
ars_id VARCHAR,
station_name VARCHAR,
geometry GEOMETRY(POINT, 5179),
legaldong_code VARCHAR);

DROP TABLE IF EXISTS 문재식.tb_seoul_bus_station_legaldong;
INSERT INTO 문재식.tb_seoul_bus_station_legaldong
(SELECT
    t.*,
    l.legaldong_code
FROM
    문재식.tb_seoul_bus_stations t
JOIN
    temporary.tb_legaldong_geometry l
    ON ST_Within(t.geometry, l.geometry)
WHERE
    length(l.legaldong_code) = 8);

-- 공간 인덱스 생성
CREATE INDEX idx_seoulbus_geom ON 문재식.tb_seoul_bus_stations USING gist (geometry);



--WHERE legaldong_code IS NULL  모든 값 매핑 완료;
WHERE legaldong_code LIKE '415%';


-- 데이터타입 확인
SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'tb_seoul_bus_station_legaldong';


-- 1. 정류장 명칭과 법정동 코드로 조인 시도
-- 총 개수 5,226,989
SELECT
  COUNT(*) AS total_rows,
  COUNT(b1.geometry) AS live_geometry_rows,
  COUNT(b2.geometry) AS work_geometry_rows,
  ROUND(COUNT(b1.geometry)::decimal / COUNT(*) * 100, 2) AS live_geometry_percent,
  ROUND(COUNT(b2.geometry)::decimal / COUNT(*) * 100, 2) AS work_geometry_percent
FROM 문재식.tb_morning_commute_od_202407 a
    -- 2.1. 주거지 정류장 관련 조인
LEFT JOIN 문재식.tb_seoul_bus_station_legaldong b1
    ON a.live_station_name = b1.station_name
    AND left(a.live_station_admin, 8) = b1.legaldong_code
    -- 2.2. 업무지 정류장 관련 조인
LEFT JOIN 문재식.tb_seoul_bus_station_legaldong b2
    ON a.work_station_name = b2.station_name
    AND left(a.work_station_admin, 8) = b2.legaldong_code;

-- 결합률 확인 50%

SELECT
  a.*, b1.geometry AS live_geometry, b2.geometry AS work_geometry
FROM 문재식.tb_morning_commute_od_202407 a
    -- 2.1. 주거지 정류장 관련 조인
LEFT JOIN 문재식.tb_seoul_bus_station_legaldong b1
    ON a.live_station_name = b1.station_name
    AND left(a.live_station_admin, 8) = b1.legaldong_code
    -- 2.2. 업무지 정류장 관련 조인
LEFT JOIN 문재식.tb_seoul_bus_station_legaldong b2
    ON a.work_station_name = b2.station_name
    AND left(a.work_station_admin, 8) = b2.legaldong_code;




-- 법정동 코드와 명칭으로 고유값 구분이 되는지 확인
SELECT live_station_id, live_station_admin, work_station_id, work_station_admin, count(*)
FROM 문재식.tb_morning_commute_od_202407
GROUP BY live_station_id, live_station_admin, work_station_id, work_station_admin
HAVING count(*) > 1;

-- 1개 이상인 건들 확인
-- 주거정류장ID와 업무정류장ID로 고유값 구분
WITH morning_commute AS
         (SELECT work_station_admin,
                 SUM(morning_commute_count)            AS morning_commute_count_sum,
                 AVG(morning_commute_average_time)     AS morning_commute_average_time,
                 AVG(morning_commute_median_time)      AS morning_commute_median_time,
                 AVG(morning_commute_average_distance) AS morning_commute_average_distance,
                 AVG(morning_commute_median_distance)  AS morning_commute_median_distance
          FROM 문재식.tb_morning_commute_od_202407
          WHERE live_station_admin = '4113511600'
          GROUP BY work_station_admin
          ORDER BY SUM(morning_commute_count) DESC
)
SELECT m.*,
       l.korean_name,
       l.geometry
FROM morning_commute m
LEFT JOIN temporary.tb_legaldong_geometry l
ON (
    CASE
        WHEN LENGTH(l.legaldong_code) = 8 THEN CONCAT(l.legaldong_code, '00')
            ELSE l.legaldong_code
        END
        =m.work_station_admin
    )
ORDER BY morning_commute_count_sum DESC;

-- 대장동 법정동 가져오기
SELECT *
FROM temporary.tb_legaldong_geometry;

-- 건물 도형
SELECT *
FROM centroid.tb_road_name_address_building_geometry_202504
WHERE road_name_address_management_number LIKE '411351164859965%';


-- 호단위
select distinct gppk, road_name_address_management_number
from building_document.tb_building_total_202506
where pnu like '4113511600%';

SELECT
    column_name,
    data_type
FROM
    information_schema.columns
WHERE
    table_name = 'tb_title_road_name_address_coordinate_202504';


-- 세대수 파악
WITH household AS (select *,
                          ROUND(household_count / sum(household_count) OVER () * 100, 2) AS household_percent
                   from building_document.tb_aggregate_title_part_202506
                   where land_number_address like '%분당구 대장동%'
                     and household_count > 0
                   order by household_count DESC)
SELECT h.road_name_address, h.building_name,
                          h.household_count, h.household_percent, r.geometry
FROM household h
LEFT JOIN centroid.tb_road_name_address_building_geometry_202504 r
    ON (h.sido_sigungu_code || LEFT(h.eupmyeondong_ri_code, 3) || RIGHT(h.road_code, 7)|| h.underground_division_code
            || LPAD(COALESCE(h.main_building_number::varchar, '0'), 5, '0') || LPAD(COALESCE(h.sub_building_number::varchar, '0'), 5, '0'))
           = r.road_name_address_management_number
ORDER BY h.household_percent DESC;

-- 건물 도형
SELECT *
FROM centroid.tb_road_name_address_building_geometry_202504;

-- 법정동 <-> 지오매트리
SELECT *
FROM temporary.tb_legaldong_geometry
-- where legaldong_code = '%';
where korean_name = '을지로2가'

select length(legaldong_code) as legal_length,
       count(distinct legaldong_code) as unique_count
from temporary.tb_legaldong_geometry
group by length(legaldong_code)
order by legal_length

select *
from temporary.tb_legaldong_geometry;

--'operation_date', 'station_id', 'station_name', 'station_type',
       'grid_id', 'legaldong_code', 'duration_type', 'total_duration_time',
       'duration_count', 'duration_average_time', 'duration_median_time',
       'geometry'
DROP TABLE 문재식.tb_morning_commute_od_202407;

DROP TABLE 문재식.tb_metropolitan_duration_time_202407;
-- 테이블 생성
CREATE TABLE 문재식.tb_metropolitan_duration_time_202407
(operation_date VARCHAR(8) NOT NULL,
station_id VARCHAR(20) NOT NULL,
station_name VARCHAR(100),
station_type VARCHAR(1),
grid_id VARCHAR(12),
legaldong_code VARCHAR(10),
duration_type VARCHAR(5),
total_duration_time INT,
duration_count INT,
duration_average_time INT,
duration_median_time INT,
geometry GEOMETRY(Point, 5179),
CONSTRAINT pk_duration_time PRIMARY KEY (operation_date, station_id, station_type, grid_id, duration_type));

SELECT COUNT(*) FROM 문재식.tb_metropolitan_morning_commute_od_202407;
SELECT COUNT(*) FROM 문재식.tb_metropolitan_evening_commute_od_202407;

SELECT COUNT(*) FROM 문재식.tb_metropolitan_duration_time_202407;

SELECT COUNT(*)
FROM ( SELECT 1 FROM 문재식.tb_metropolitan_duration_time_202407
GROUP BY station_id, station_type, grid_id);
WHERE duration_count > 50 AND duration_type = 'live'
ORDER BY duration_average_time DESC
LIMIT 100;

SELECT * FROM grid.tb_grid_100m;

SELECT work_station_legaldong,
       SUM(morning_commute_count) AS count
FROM 문재식.tb_metropolitan_morning_commute_od_202407
GROUP BY work_station_legaldong
ORDER BY SUM(morning_commute_count) DESC
LIMIT 20;

-- 강남구 업무지 출근 기준 예시 쿼리
WITH gangnam AS(
    SELECT live_station_legaldong,
           SUM(morning_daily_commute_count) AS morning_commute_count_sum,
           -- 통행량 비중 컬럼 추가
            ROUND(
            SUM(morning_daily_commute_count)*100
            / SUM(SUM(morning_daily_commute_count)) OVER (), 2
            ) AS morning_commute_percent,
           ROUND(AVG(morning_commute_average_time), 2) AS morning_commute_averge_time,
           ROUND(AVG(morning_commute_median_time), 2) AS morning_commute_median_time,
           ROUND(AVG(morning_commute_average_distance), 2) AS morning_commute_average_distance,
           ROUND(AVG(morning_commute_median_distance), 2) AS morning_commute_median_distance
    FROM 문재식.tb_metropolitan_morning_commute_od_202407
    WHERE work_station_legaldong = '1168010100'
    GROUP BY live_station_legaldong
)
-- 법정동 정보: 10자리
SELECT l.legaldong_name,
       g.*,
       lg.geometry
FROM gangnam g
LEFT JOIN code.tb_realdeal_legaldong_code l
    ON l.legaldong_code = g.live_station_legaldong
LEFT JOIN temporary.tb_legaldong_geometry lg
    ON LEFT(l.legaldong_code,8) = lg.legaldong_code
ORDER BY morning_commute_percent DESC;

SELECT * FROM temporary.tb_legaldong_geometry
WHERE legaldong_code LIKE '11680101%';

SELECT * FROM 문재식.tb_metropolitan_evening_commute_od_202407
WHERE work_station_name = '시청'
ORDER BY evening_daily_commute_count desc;

-- 강남구 업무지 출근 기준 예시 쿼리
WITH gangnam AS(
    SELECT live_station_legaldong,
           SUM(morning_daily_commute_count) AS morning_commute_count_sum,
           -- 통행량 비중 컬럼 추가
            ROUND(
            SUM(morning_daily_commute_count)*100
            / SUM(SUM(morning_daily_commute_count)) OVER (), 2
            ) AS morning_commute_percent,
           ROUND(AVG(morning_commute_average_time)/60) AS morning_commute_averge_time,
           ROUND(AVG(morning_commute_median_time)/60) AS morning_commute_median_time,
           ROUND(AVG(morning_commute_average_distance), 2) AS morning_commute_average_distance,
           ROUND(AVG(morning_commute_median_distance), 2) AS morning_commute_median_distance
    FROM 문재식.tb_metropolitan_morning_commute_od_202407
    WHERE work_station_legaldong = '1168010100'
    GROUP BY live_station_legaldong
)
-- 법정동 정보: 10자리
SELECT l.legaldong_name,
       g.*,
       lg.geometry
FROM gangnam g
LEFT JOIN code.tb_realdeal_legaldong_code l
    ON l.legaldong_code = g.live_station_legaldong
LEFT JOIN temporary.tb_legaldong_geometry lg
    ON LEFT(l.legaldong_code,8) = lg.legaldong_code
WHERE l.legaldong_name LIKE '경기도%'
ORDER BY g.morning_commute_averge_time;

select * from 문재식.tb_metropolitan_morning_commute_od_202407
order by morning_daily_commute_count desc;

-- 권한 수정
ALTER DEFAULT PRIVILEGES IN SCHEMA 문재식 GRANT SELECT ON TABLES TO bv_dataplatform;

ALTER DEFAULT PRIVILEGES IN SCHEMA 문재식 GRANT SELECT ON TABLES TO postgres;

GRANT SELECT ON 문재식.tb_metropolitan_morning_commute_od_202407 TO "sunjaekim@bigvalue.co.kr";
GRANT SELECT ON 문재식.tb_metropolitan_evening_commute_od_202407 TO "sunjaekim@bigvalue.co.kr";
GRANT SELECT ON 문재식.tb_metropolitan_duration_time_202407 TO "sunjaekim@bigvalue.co.kr";


-- 사사분면 가설
--- 중심상권: 체류인원 높음, 체류시간 높음(2시간이상)
SELECT *
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE operation_date = '20240707' AND duration_type <> 'live'
ORDER BY total_duration_time DESC, duration_count DESC
LIMIT 100;


-- 환승중심지: 체류인원 높음, 체류시간 낮음
SELECT *
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE operation_date = '20240707' AND duration_type <> 'live'
ORDER BY total_duration_time ASC, duration_count DESC
LIMIT 100;

-- - 주말출근지?: 체류인원 낮음, 체류시간 높음
SELECT *
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE operation_date = '20240707' AND duration_type = 'work'
ORDER BY total_duration_time DESC, duration_count ASC
LIMIT 100;

SELECT *
FROM 현해준.tb_강남_서초_신한카드_매출;

SELECT *
FROM 문재식.tb_metropolitan_morning_commute_od_202407


SELECT *
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE station_name = '시청' AND station_type = 'T';


-- 1. 10분이내
-- 300m 버퍼 생성하기
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom)
-- 300M 버퍼 내에 있는 정류장 집계
SELECT ST_Convexhull(ST_Collect(m.live_station_geometry))
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 600;


-- 2. 30분이내
-- 300m 버퍼 생성하기
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom)
-- 300M 버퍼 내에 있는 정류장 집계
SELECT ST_Convexhull(ST_Collect(m.live_station_geometry))
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 1800;

-- 3. 1시간이내
-- 300m 버퍼 생성하기
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom)
-- 300M 버퍼 내에 있는 정류장 집계
SELECT ST_Convexhull(ST_Collect(m.live_station_geometry))
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 3600;

-- 4. 90분 이내
-- 300m 버퍼 생성하기
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom)
-- 300M 버퍼 내에 있는 정류장 집계
SELECT ST_Convexhull(ST_Collect(m.live_station_geometry))
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 5400;

-- 5. 120분 이내
-- 300m 버퍼 생성하기
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom)
-- 300M 버퍼 내에 있는 정류장 집계
SELECT ST_Convexhull(ST_Collect(m.live_station_geometry))
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 7200;


-- 거리/시간 이상치 확인
-- 10분거리 이지만 거리가 지나치게 먼 곳
WITH base_buffer AS (
SELECT ST_Buffer(
               ST_Transform(
                   ST_SetSRID(
                           ST_MakePoint(126.976296, 37.563641), 4326), 5179
),
                            300) AS geom),
-- 300M 버퍼 내에 있는 정류장 집계
step_01 AS(
    SELECT *,
       -- 원점(빅밸류)로부터의 거리 컬럼
       ST_Distance(
               m.live_station_geometry, ST_Transform(
           ST_SetSRID(
                   ST_MakePoint(126.976296, 37.563641), 4326), 5179)) AS distance_from_bv
FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
JOIN base_buffer b
    ON ST_Within(m.work_station_geometry, b.geom)
WHERE m.morning_commute_average_time < 1800
ORDER BY morning_commute_average_distance DESC
    )
SELECT live_station_id,
       live_station_name,
       live_station_grid_id,
       live_station_type,
       round(distance_from_bv::numeric, 2) AS distance_from_bv,
       morning_daily_commute_count,
       morning_commute_average_time,
       morning_commute_median_time,
       morning_commute_average_distance,
       morning_commute_median_distance,
       live_station_geometry
FROM step_01
ORDER BY distance_from_bv DESC;

-- 지하철 OD 체크
SELECT live_station_name,
       work_station_name,
       COUNT(*)
FROM 문재식.tb_metropolitan_morning_commute_od_202407
WHERE live_station_type = 'T' AND work_station_type = 'T'
GROUP BY live_station_name, work_station_name
HAVING COUNT(*)>1
ORDER BY COUNT(*) DESC;

-- 고유값: 150, 426, 1001, 4201, 1251
SELECT *
FROM 문재식.tb_metropolitan_morning_commute_od_202407
WHERE live_station_name = '서울' AND work_station_name = '서울'
AND live_station_type = 'T' AND work_station_type = 'T';

WITH filtered AS (
                SELECT *
                FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
                JOIN (
                    SELECT ST_Buffer(
                               ST_Transform(
                                   ST_SetSRID(
                                       ST_MakePoint(126.976296, 37.563641), 4326), 5179
                               ),
                               300) AS geom
                ) b
                  ON ST_Within(m.work_station_geometry, b.geom)
                WHERE m.morning_commute_average_time < :time
            )
            SELECT ST_AsGeoJSON(ST_Transform(m.work_station_geometry, 4326)), m.work_station_name
            FROM filtered m;


-- 공간 인덱스 생성
CREATE INDEX idx_live_station_geometry ON 문재식.tb_metropolitan_morning_commute_od_202407 USING GIST(live_station_geometry);
CREATE INDEX idx_work_station_geometry ON 문재식.tb_metropolitan_morning_commute_od_202407 USING GIST(live_station_geometry);

CREATE INDEX idx_live_station_geometry ON 문재식.tb_metropolitan_evening_commute_od_202407 USING GIST(live_station_geometry);
CREATE INDEX idx_work_station_geometry ON 문재식.tb_metropolitan_evening_commute_od_202407 USING GIST(live_station_geometry);

select live_station_id, live_station_name, live_station_grid_id, morning_daily_commute_count
from 문재식.tb_metropolitan_morning_commute_od_202407
where work_station_legaldong = '1168010100'
group by live_station_id, live_station_name, live_station_grid_id
         order by morning_daily_commute_count desc;



WITH filtered AS (
                SELECT *
                FROM 문재식.tb_metropolitan_morning_commute_od_202407 m
                JOIN (
                    SELECT ST_Buffer(
                               ST_Transform(
                                   ST_SetSRID(
                                       ST_MakePoint(126.976296, 37.563641), 4326), 5179
                               ),
                               300) AS geom
                ) b
                  ON ST_Within(m.work_station_geometry, b.geom))
            SELECT ST_AsGeoJSON(
                           ST_Transform(
                                   m.live_station_geometry, 4326)),
                   m.live_station_name,
                   m.morning_commute_average_time,
                   m.morning_commute_median_time,
                   m.morning_commute_average_distance,
                   m.morning_commute_median_distance
            FROM filtered m;

select * from 문재식.tb_seoul_bus_stations;

SELECT *
FROM bv_platform_vision.tb_merged_court_auction;

SELECT *
FROM 문재식.tb_metropolitan_morning_commute_od_202407;

DROP TABLE 문재식.tb_metropolitan_voronoi_cluster;
CREATE TABLE 문재식.tb_metropolitan_voronoi_cluster
(id VARCHAR(5) PRIMARY KEY,
geometry GEOMETRY(POLYGON, 5179));

SELECT *
FROM 문재식.tb_metropolitan_voronoi_cluster;

SELECT *
FROM bv_platform_data_mart.tb_bv_platform_naver_saleitem;

-- 원본 테이블 : 매주 토요일 수집 전 테이블을 truncate하고 재수집
-- 단지 : temporary.tb_naver_complex
-- 동 : temporary.tb_naver_dong
-- 호 : temporary.tb_naver_ho
-- 평형 : temporary.tb_naver_pyeong
-- 매물 : temporary.tb_naver_saleitem

-- 누적 테이블 : 매주 토요일 수집 완료된 원본 데이터를 standard_date 기준으로 정제 및 누적
-- 단지 : privateoperation_data.tb_naver_complex
-- 호(동포함) : privateoperation_data.tb_naver_ho
-- 평형 : privateoperation_data.tb_naver_pyeong
-- 매물 : privateoperation_data.tb_naver_saleitem

SELECT *
FROM privateoperation_data.tb_naver_saleitem
WHERE deal_division_name ='매매';

-- TOP 30 출근하차 클러스터 찾아보기
SELECT b.cluster_id,
       b.station_name,
       SUM(a.morning_daily_commute_count) AS SUM_COUNT
FROM 문재식.tb_metropolitan_morning_commute_od_202407 a
JOIN 문재식.tb_metropolitan_voronoi_cluster b
    ON ST_Intersects(a.work_station_geometry, b.geometry)
GROUP BY b.cluster_id,
         b.station_name
ORDER BY SUM(morning_daily_commute_count) DESC
LIMIT 30;




---------