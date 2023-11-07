-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;
set tez.am.container.reuse.enabled=false;

set hivevar:db_name = di_crowd ;

INSERT OVERWRITE TABLE ${hivevar:db_name}.kickboard_available_region PARTITION (exec_ym=${hivevar:exec_ym})
    SELECT rpad(district_id, 10 , "0") AS sig_code,
           r_c.monthly_ride_count
    FROM (
            SELECT substring(riding_region_code, 1,5) AS region_code , COUNT(riding_region_code) AS monthly_ride_count
            FROM ${hivevar:db_name}.kickboard_session
            WHERE exec_dt BETWEEN concat(${hivevar:exec_ym}, '01') AND ${hivevar:exec_dt}
                    AND ride_yn = 1
            GROUP BY substring(riding_region_code, 1, 5)
            HAVING COUNT(riding_region_code) >= 100
         ) r_c
    LEFT JOIN (
                    SELECT `dec`(ct_gun_gu_cd )AS district_id,
                         CASE
                                WHEN `dec`(ct_pvc_nm) = "서울" THEN CONCAT("서울특별시", ' ',`dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "부산" THEN CONCAT("부산광역시", ' ',`dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "대구" THEN CONCAT("대구광역시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "인천" THEN CONCAT("인천광역시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "광주" THEN CONCAT("광주광역시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "대전" THEN CONCAT("대전광역시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "울산" THEN CONCAT("울산광역시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "세종" THEN CONCAT("세종특별자치시", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "경기" THEN CONCAT("경기도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "강원" THEN CONCAT("강원도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "충북" THEN CONCAT("충청북도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "충남" THEN CONCAT("충청남도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "전북" THEN CONCAT("전라북도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "전남" THEN CONCAT("전라남도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "경북" THEN CONCAT("경상북도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "경남" THEN CONCAT("경상남도", ' ', `dec`(ct_gun_gu_nm) )
                                WHEN `dec`(ct_pvc_nm) = "제주" THEN CONCAT("제주특별자치도", ' ', `dec`(ct_gun_gu_nm) )
                        END AS ctp_sig_name
                    FROM wind_tmt.mmkt_ldong_cd_d
                    WHERE `dec`(up_myun_dong_nm) = "#"
                        AND `dec`(ct_gun_gu_nm) != "#"
               ) r_n
    ON r_c.region_code = r_n.district_id
