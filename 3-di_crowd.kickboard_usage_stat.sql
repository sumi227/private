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
set hive.map.aggr=true;

set hivevar:db_name = di_crowd ;

--2022.07.11 with문 삭제

INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_usage_stat PARTITION (exec_ym=${hivevar:exec_ym})
    SELECT duration_distance.district_id AS sig_code,
           avg_riding_distance, avg_riding_duration,
           avg_monthly_riding_count, avg_monthly_use_app_count
    FROM (
            SELECT COALESCE(district_id, "0000000000") AS district_id,
                   AVG(session_distance_km) AS avg_riding_distance, AVG(session_residence_second) AS avg_riding_duration,
                   COUNT(*) / COUNT( DISTINCT svc_mgmt_num ) AS avg_monthly_riding_count
            FROM (
                   SELECT svc_mgmt_num, service_name, RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id,
                     session_distance_km, session_residence_second
                     FROM ${hivevar:db_name}.kickboard_session
                     WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                            AND ride_yn = 1
                            AND RPAD(substr(riding_region_code, 1, 5), 10, "0") IN (
                                                                      SELECT sig_code
                                                                      FROM ${hivevar:db_name}.kickboard_available_region
                                                                      WHERE exec_ym = ${hivevar:exec_ym}
                                                               )
            ) t1
            GROUP BY district_id WITH ROLLUP
         ) duration_distance
    LEFT JOIN (
                SELECT COALESCE(district_id, "0000000000") AS district_id, AVG(avg_monthly_use_app_count) AS avg_monthly_use_app_count
                FROM (
                        SELECT district_id, svc_mgmt_num, COUNT( DISTINCT service_name ) AS avg_monthly_use_app_count
                        FROM (
                            SELECT svc_mgmt_num, service_name, RPAD(substr(riding_region_code, 1, 5), 10, "0") AS district_id,
                                   session_distance_km, session_residence_second
                                   FROM ${hivevar:db_name}.kickboard_session
                                   WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                                   AND ride_yn = 1
                                   AND RPAD(substr(riding_region_code, 1, 5), 10, "0") IN (
                                                                                    SELECT sig_code
                                                                                    FROM ${hivevar:db_name}.kickboard_available_region
                                                                                    WHERE exec_ym = ${hivevar:exec_ym}
                                                                             )
                        ) t1
                        GROUP BY district_id, svc_mgmt_num
                      ) region_user_count
                GROUP BY district_id WITH ROLLUP
              ) region_avg_app_count
    ON duration_distance.district_id = region_avg_app_count.district_id

