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


INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_market_stat PARTITION (exec_ym=${hivevar:exec_ym})
    SELECT RPAD( region_ms.district_id, 10 , "0") AS sig_code,
           region_ms.service_name AS app_service_name,
           region_ms.marketshare,
           region_ms.monthly_ride_count,
           region_rr.reuse_rate
    FROM (
            SELECT district_id,
                   service_name,
                   region_app_use_count / SUM(region_app_use_count) OVER(PARTITION BY district_id) AS marketshare,
                   region_app_use_count AS monthly_ride_count
            FROM (
                    SELECT district_id,
                           service_name,
                           COUNT(*) AS region_app_use_count
                    FROM (
                            SELECT service_name,
                                   substr(riding_region_code, 1, 5) AS district_id
                            FROM ${hivevar:db_name}.kickboard_session
                            WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                                AND ride_yn = 1
                                AND rpad(substr(riding_region_code, 1, 5), 10, "0")  IN (
                                                                            SELECT sig_code
                                                                            FROM ${hivevar:db_name}.kickboard_available_region
                                                                            WHERE exec_ym = ${hivevar:exec_ym}
                                                                          )
                          ) session
                    GROUP BY district_id, service_name
                 ) ms
          ) region_ms
     LEFT JOIN (
                    SELECT district_id, service_name,
                           -- 재주문율 = 향후 1개월간 재주문을 한 고객 수/ 첫달 주문 고객 수
                           -- 첫달 주문 고객 수가 0인 경우 NULL값 반환, 0이 아닌 경우 FLOAT으로 결과값 변환
                           CASE
                            WHEN MAX(two_month_ago_client_count) < 0 THEN NULL
                            ELSE SUM(is_reuse) / MAX(two_month_ago_client_count)
                           END AS reuse_rate
                    FROM (
                            SELECT two_month_ago_session.*,
                                   SIZE(two_month_ago_client_set) AS two_month_ago_client_count,
                                   CASE WHEN ARRAY_CONTAINS(two_month_ago_client_set, svc_mgmt_num) THEN 1 ELSE 0 END AS is_reuse
                            FROM (
                                    SELECT substr(riding_region_code, 1, 5) AS district_id, service_name, COLLECT_SET(svc_mgmt_num) AS two_month_ago_client_set
                                    FROM ${hivevar:db_name}.kickboard_session
                                    WHERE exec_dt BETWEEN regexp_replace(ADD_MONTHS( TRUNC( CONCAT( SUBSTR(${hivevar:exec_dt} , 1, 4) , '-', SUBSTR(${hivevar:exec_dt} , 5, 2), '-', SUBSTR(${hivevar:exec_dt} , 7, 2) )  , "MM"), -1), '-', '')
                                          AND regexp_replace(LAST_DAY( ADD_MONTHS( TRUNC( CONCAT( SUBSTR(${hivevar:exec_dt} , 1, 4) , '-', SUBSTR(${hivevar:exec_dt} , 5, 2), '-', SUBSTR(${hivevar:exec_dt} , 7, 2) )  , "MM"), -1) ), '-', '')
                                        AND ride_yn = 1
                                        AND rpad(substr(riding_region_code, 1, 5), 10, "0") IN (
                                                                    SELECT sig_code
                                                                    FROM ${hivevar:db_name}.kickboard_available_region
                                                                    WHERE exec_ym = ${hivevar:exec_ym}
                                                                  )
                                    GROUP BY substr(riding_region_code, 1, 5), service_name
                                  ) two_month_ago_session
                             LEFT JOIN (
                                        SELECT DISTINCT substr(riding_region_code, 1, 5) AS district_id, service_name, svc_mgmt_num
                                        FROM ${hivevar:db_name}.kickboard_session
                                        WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                                              AND ride_yn = 1
                                        ) prev_month_session
                            ON two_month_ago_session.district_id = prev_month_session.district_id
                                AND two_month_ago_session.service_name = prev_month_session.service_name
                          ) reuse_table
                    GROUP BY district_id, service_name
               ) region_rr
     ON region_ms.district_id = region_rr.district_id
        AND region_ms.service_name = region_rr.service_name


