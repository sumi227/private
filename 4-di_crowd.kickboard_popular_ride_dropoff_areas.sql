set hive.vectorized.execution.enabled = false;
set hive.tez.container.size = 8000;
set hive.exec.parallel=true;
set hive.exec.orc.zerocopy=false;

set hive.tez.java.opts = -Xmx8458m;
set tez.am.resource.memory.mb = 10572;
set tez.am.java.opts = Xmx8458m;

set hive.support.quoted.identifiers=none;
set hive.mapred.mode=unstrict;

set hivevar:db_name = di_crowd ;

WITH ride_middle_table AS (
    SELECT RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id,
           riding_region_latitude AS latitude,
           riding_region_longitude AS longitude
    FROM ${hivevar:db_name}.kickboard_session
    WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
        AND ride_yn = 1
        AND RPAD(substr(riding_region_code, 1, 5), 10, "0") IN (
                                                    SELECT sig_code
                                                    FROM ${hivevar:db_name}.kickboard_available_region
                                                    WHERE exec_ym = ${hivevar:exec_ym}
                                                  )
    ), ride_table AS (
    SELECT main.*,
           CAST( ( main.latitude - region.region_min_latitude ) / 0.001 AS INT ) AS lat_grid,
           CAST( ( main.longitude - region.region_min_longitude ) / 0.001 AS INT ) AS long_grid
    FROM (
            SELECT district_id, latitude, longitude
            FROM ride_middle_table
         ) main
    LEFT JOIN (
                SELECT district_id,
                       MIN(latitude) AS region_min_latitude,
                       MIN(longitude) AS region_min_longitude
                FROM ride_middle_table
                GROUP BY district_id
              ) region
    ON main.district_id = region.district_id
    ), dropoff_middle_table AS (
    SELECT RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id,
           dropoff_region_latitude AS latitude,
           dropoff_region_longitude AS longitude
    FROM ${hivevar:db_name}.kickboard_session
    WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
        AND ride_yn = 1
        AND RPAD(substr(riding_region_code, 1, 5), 10, "0") IN (
                                                    SELECT sig_code
                                                    FROM ${hivevar:db_name}.kickboard_available_region
                                                    WHERE exec_ym = ${hivevar:exec_ym}
                                                  )
    ), dropoff_table AS (
    SELECT main.*,
           CAST( ( main.latitude - region.region_min_latitude ) / 0.001 AS INT ) AS lat_grid,
           CAST( ( main.longitude - region.region_min_longitude ) / 0.001 AS INT ) AS long_grid
    FROM (
            SELECT district_id, latitude, longitude
            FROM dropoff_middle_table
         ) main
    LEFT JOIN (
                SELECT district_id,
                       MIN(latitude) AS region_min_latitude,
                       MIN(longitude) AS region_min_longitude
                FROM dropoff_middle_table
                GROUP BY district_id
              ) region
    ON main.district_id = region.district_id
)

INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_popular_ride_dropoff_areas PARTITION (exec_ym=${hivevar:exec_ym})
      SELECT freq_grid.district_id AS sig_code,
             "ride" AS ride_type,
             freq_grid.ranking,
             grid_table.region_min_lat + 0.001 * lat_grid + 0.0005 AS area_lat,
             grid_table.region_min_long + 0.001 * long_grid + 0.0005 AS area_lng
      FROM (
            SELECT district_id, lat_grid, long_grid, ranking
            FROM (
                    SELECT district_id, lat_grid, long_grid,
                           ROW_NUMBER() OVER(PARTITION BY district_id ORDER BY grid_cnt DESC) AS ranking
                    FROM (
                            SELECT district_id, lat_grid, long_grid, COUNT(*) AS grid_cnt
                            FROM ride_table
                            GROUP BY district_id, lat_grid, long_grid
                         ) count_grid
                  ) grid_top5
            WHERE ranking <= 5
           ) freq_grid
      LEFT JOIN (
                   SELECT district_id,
                          MIN(latitude) AS region_min_lat, MIN(longitude) AS region_min_long
                   FROM ride_middle_table
                   GROUP BY district_id
                ) grid_table
      ON freq_grid.district_id = grid_table.district_id
      UNION ALL
      SELECT freq_grid.district_id AS sig_code,
             "dropoff" AS ride_type,
             freq_grid.ranking,
             grid_table.region_min_lat + 0.001 * lat_grid + 0.0005 AS area_lat,
             grid_table.region_min_long + 0.001 * long_grid + 0.0005 AS area_lng
      FROM (
            SELECT district_id, lat_grid, long_grid, ranking
            FROM (
                    SELECT district_id, lat_grid, long_grid,
                           ROW_NUMBER() OVER(PARTITION BY district_id ORDER BY grid_cnt DESC) AS ranking
                    FROM (
                            SELECT district_id, lat_grid, long_grid, COUNT(*) AS grid_cnt
                            FROM dropoff_table
                            GROUP BY district_id, lat_grid, long_grid
                         ) count_grid
                  ) grid_top5
            WHERE ranking <= 5
           ) freq_grid
      LEFT JOIN (
                   SELECT district_id,
                          MIN(latitude) AS region_min_lat, MIN(longitude) AS region_min_long
                   FROM dropoff_middle_table
                   GROUP BY district_id
                ) grid_table
      ON freq_grid.district_id = grid_table.district_id
