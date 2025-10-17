-- 환승역 예시
-- 'T'이면서 이름인데 동시에 여러 station_id를 가지는 경우
SELECT operation_date,
       station_name,
       duration_type,
       STRING_AGG(station_id::text, ',') AS station_ids,
       COUNT(DISTINCT station_id) AS station_id_count
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE station_type = 'T'
GROUP BY operation_date, station_name, duration_type
HAVING COUNT(DISTINCT station_id)>1
ORDER BY station_id_count desc;

-- 개별 데이터
SELECT *
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE station_name LIKE '군자%' AND operation_date = '20240701'
  AND station_type = 'T' AND duration_type = 'live';

-- duration_count는 더하고
-- duration_average_time은 평균 재합산 공식 SUM(duration_count * duration_average_time)/SUM(duration_count)

-- GROUPBY test
SELECT operation_date,
       MIN(station_id) AS station_id,
       station_name,
       station_type
FROM 문재식.tb_metropolitan_duration_time_202407
WHERE station_type = 'T'
GROUP BY operation_date, station_name, duration_type, station_type;


-- 1. 두 개 이상인 그룹의 key만 뽑기
-- 644개의 지하철 정류장 중 98개의 환승역을 기준으로 수행
-- 25377 -> 11697개의 행
select count(distinct(station_name)) from 문재식.tb_metropolitan_duration_time_202407 where station_type = 'T';
WITH candidates AS (
  SELECT
    operation_date,
    station_name,
    duration_type,
    station_type
  FROM 문재식.tb_metropolitan_duration_time_202407
  WHERE station_type = 'T'
  GROUP BY operation_date, station_name, duration_type, station_type
  HAVING COUNT(*) > 1
)
select count(*) from 문재식.tb_metropolitan_duration_time_202407 a
JOIN candidates b
    ON a.operation_date = b.operation_date
    AND a.station_name = b.station_name
    AND a.duration_type = b.duration_type
    AND a.station_type = b.station_type;


WITH candidates AS (SELECT operation_date,
                                          station_name,
                                          duration_type,
                                          station_type
                                   FROM 문재식.tb_metropolitan_duration_time_202407
                                   WHERE station_type = 'T'
                                   GROUP BY operation_date, station_name, duration_type, station_type
                                   HAVING COUNT(*) > 1)
-- 2. 대표 row 한 줄만 남기기
               SELECT t.operation_date,
                      t.station_name,
                      t.duration_type,
                      t.station_type,
                      t.station_id,
                      t.grid_id
               FROM (SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY operation_date, station_name, duration_type, station_type
                                ORDER BY station_id
                                ) AS rn
                     FROM 문재식.tb_metropolitan_duration_time_202407
                     WHERE station_type = 'T') t
                        JOIN candidates c
                             ON t.operation_date = c.operation_date
                                 AND t.station_name = c.station_name
                                 AND t.duration_type = c.duration_type
                                 AND t.station_type = c.station_type
                        WHERE t.rn = 1;



WITH candidates AS (
  SELECT
    operation_date,
    station_name,
    duration_type,
    station_type
  FROM 문재식.tb_metropolitan_duration_time_202407
  WHERE station_type = 'T'
  GROUP BY operation_date, station_name, duration_type, station_type
  HAVING COUNT(*) > 1
),
-- 집계함수들 정리
agg AS (
  SELECT
    operation_date,
    station_name,
    duration_type,
    station_type,
    SUM(duration_count) AS total_count,
    SUM(duration_count * duration_average_time)::float / NULLIF(SUM(duration_count), 0) AS weighted_avg_time
    -- 추가 집계 붙이면 됨
  FROM 문재식.tb_metropolitan_duration_time_202407
  WHERE station_type = 'T'
  GROUP BY operation_date, station_name, duration_type, station_type
),
-- 첫번째거만 남기는 행
mainrow AS (
  SELECT
    operation_date,
    station_name,
    duration_type,
    station_type,
    station_id,
    grid_id,
    ROW_NUMBER() OVER (
      PARTITION BY operation_date, station_name, duration_type, station_type
      ORDER BY station_id
    ) AS rn
  FROM 문재식.tb_metropolitan_duration_time_202407
  WHERE station_type = 'T'
)
SELECT
  m.operation_date,
  m.station_name,
  m.duration_type,
  m.station_type,
  m.station_id,
  m.grid_id,
  a.total_count,
  a.weighted_avg_time
FROM mainrow m
JOIN candidates c
  ON m.operation_date = c.operation_date
 AND m.station_name = c.station_name
 AND m.duration_type = c.duration_type
 AND m.station_type = c.station_type
JOIN agg a
  ON m.operation_date = a.operation_date
 AND m.station_name = a.station_name
 AND m.duration_type = a.duration_type
 AND m.station_type = a.station_type
WHERE m.rn = 1
;



select * from 문재식.tb_metropolitan_morning_commute_od_202407;

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
                   m.morning_commute_median_distance,
                    m.morning_daily_commute_count
            FROM filtered m;

-- 단지형 아파트 + 보로노이 폴리곤 + 정류장
WITH station_cluster AS (
  SELECT
    a.station_id,
    a.station_name,
    a.geometry,
    b.cluster_id
  FROM 문재식.temporary_metropolitan_station_geometry AS a
  JOIN 문재식.tb_metropolitan_voronoi_cluster AS b
    ON ST_Intersects(a.geometry, b.geometry)
),
danji_centroid AS (
  -- 단지 폴리곤의 중심점만 빼놔서 반복 계산 방지
  SELECT
    c.complex_key,
    c.complex_name,
    c.road_name_address_management_number,
    c.road_name_address,
    c.complex_household_count,
    c.complex_household_ratio,
    c.geometry,
    c.cluster_id,
    ST_Centroid(c.geometry) AS danji_centroid_geom
  FROM 문재식.tb_metropolitan_danji_voronoi_cluster AS c
)
SELECT
    dc.complex_key,
    dc.complex_name,
    dc.road_name_address_management_number,
    dc.road_name_address,
    dc.complex_household_count,
    dc.complex_household_ratio,
    dc.geometry,
    dc.cluster_id,
    d.station_id,
    d.station_name,
  -- 1) 거리 계산
  ST_Distance(dc.danji_centroid_geom, d.geometry) AS dist_m,
  -- 2) 지수 감쇠 가중치에 따라 사람이 인지할만한 통근량로 떨어져야하는데 B값의 기준이 없음
  EXP(-0.001 * ST_Distance(dc.danji_centroid_geom, d.geometry)) AS access_weight
FROM danji_centroid AS dc
JOIN station_cluster AS d
  ON dc.cluster_id = d.cluster_id::INT;





