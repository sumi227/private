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

INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_riding_points PARTITION (exec_ym=${hivevar:exec_ym})
    SELECT district_id AS sig_code, is_weekday AS holiday_yn, hh,
           COLLECT_LIST(riding_latitude) AS riding_latitude_points,
           COLLECT_LIST(riding_longitude) AS riding_longitude_points
    FROM (
            SELECT RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id, riding_latitude, riding_longitude,
                   CASE
                    WHEN FROM_UNIXTIME(UNIX_TIMESTAMP(riding_time), 'E') IN ("Sat", "Sun") THEN "N"
                    ELSE "Y"
                   END AS is_weekday,
                  HOUR(riding_time) AS hh
            FROM ${hivevar:db_name}.kickboard_session
            LATERAL VIEW POSEXPLODE(riding_latitude_points) rlat AS pos_rlat, riding_latitude
            LATERAL VIEW POSEXPLODE(riding_longitude_points) rlong AS pos_rlong, riding_longitude
            LATERAL VIEW POSEXPLODE(riding_timestamp) rtime AS pos_time, riding_time
            WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                AND ride_yn = 1
                AND pos_rlat = pos_rlong
                AND pos_rlong = pos_time
         ) middle_table
     WHERE district_id IN (
                            SELECT sig_code
                            FROM ${hivevar:db_name}.kickboard_available_region
                            WHERE exec_ym = ${hivevar:exec_ym}
                          )
     GROUP BY district_id, is_weekday, hh


