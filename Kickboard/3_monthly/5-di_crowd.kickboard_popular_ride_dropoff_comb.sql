-- config
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

WITH base_table AS (
        SELECT RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id,
               riding_region_latitude AS riding_latitude,
               riding_region_longitude AS riding_longitude,
               dropoff_region_latitude AS dropoff_latitude,
               dropoff_region_longitude AS dropoff_longitude
        FROM ${hivevar:db_name}.kickboard_session
        WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
            AND ride_yn = 1
            AND RPAD(substr(riding_region_code, 1, 5), 10, "0") IN (
                                    SELECT sig_code
                                    FROM ${hivevar:db_name}.kickboard_available_region
                                    WHERE exec_ym = ${hivevar:exec_ym}
                                  )
        ), middle_table AS (
        SELECT main.*,
               CAST( ( main.riding_latitude - region.region_min_latitude ) / 0.001 AS INT ) AS riding_lat_grid,
               CAST( ( main.riding_longitude - region.region_min_longitude ) / 0.001 AS INT ) AS riding_long_grid,
               CAST( ( main.dropoff_latitude - region.region_min_latitude ) / 0.001 AS INT ) AS dropoff_lat_grid,
               CAST( ( main.dropoff_longitude - region.region_min_longitude ) / 0.001 AS INT ) AS dropoff_long_grid
        FROM (
                SELECT district_id,
                       riding_latitude, riding_longitude,
                       dropoff_latitude, dropoff_longitude
                FROM base_table
             ) main
        LEFT JOIN (
                    SELECT district_id,
                           MIN(riding_latitude) AS region_min_latitude,
                           MIN(riding_longitude) AS region_min_longitude
                    FROM base_table
                    GROUP BY district_id
                  ) region
        ON main.district_id = region.district_id
        )
INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_popular_ride_dropoff_comb PARTITION (exec_ym=${hivevar:exec_ym})
    SELECT freq_grid.district_id AS sig_code,
           freq_grid.ranking,
           grid_table.region_min_lat + 0.001 * riding_lat_grid + 0.0005 AS ride_area_lat,
           grid_table.region_min_long + 0.001 * riding_long_grid + 0.0005 AS ride_area_lng,
           grid_table.region_min_lat + 0.001 * dropoff_lat_grid + 0.0005 AS dropoff_area_lat,
           grid_table.region_min_long + 0.001 * dropoff_long_grid + 0.0005 AS dropoff_area_lng
    FROM (
            SELECT district_id, riding_lat_grid, riding_long_grid, dropoff_lat_grid, dropoff_long_grid, grid_cnt, ranking
            FROM (
                    SELECT district_id, riding_lat_grid, riding_long_grid, dropoff_lat_grid, dropoff_long_grid, grid_cnt,
                           ROW_NUMBER() OVER(PARTITION BY district_id ORDER BY grid_cnt DESC) AS ranking
                    FROM (
                            SELECT district_id, riding_lat_grid, riding_long_grid, dropoff_lat_grid, dropoff_long_grid, COUNT(*) AS grid_cnt
                            FROM middle_table
                            GROUP BY district_id, riding_lat_grid, riding_long_grid, dropoff_lat_grid, dropoff_long_grid
                         ) count_grid
                  ) grid_comb
            WHERE ranking <= 3
         ) freq_grid
    LEFT JOIN (
                SELECT district_id,
                       MIN(riding_latitude) AS region_min_lat, MIN(riding_longitude) AS region_min_long
                FROM base_table
                GROUP BY district_id
              ) grid_table
    ON freq_grid.district_id = grid_table.district_id

