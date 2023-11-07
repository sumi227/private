
--####################################################################################################
--## Project: PUZZLE - Insight Report 
--## Script purpose: Insight Report (POI)
--## Date: 2022-11-9
--####################################################################################################

-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set hive.tez.container.size = 40960;
set hive.exec.orc.split.strategy = BI;
set hive.support.quoted.identifiers = none ;
set hive.compute.query.using.stats=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.auto.convert.join=false; 

set hivevar:dt = ${hivevar:exec_dt};
set hivevar:db_name = di_crowd ;
set hivevar:start_dt = '20220901';
set hivevar:end_dt ='20220930';
set hivevar:start_dt_6m = '20220101';


-- drop table default.smkim_insight_kickboard_usage_summary2 ; 
-- create table default.smkim_insight_kickboard_usage_summary2 as 

-- select 
--     distinct ct_nm, med_move_dist, med_velocity
-- from (
--     select 
--     t1.*
--     , t4.ct_nm 
--     , percentile_approx(session_distance_km, 0.50) over(partition by riding_region_code) as med_move_dist
--     , percentile_approx(session_velocity, 0.50) over(partition by riding_region_code) as med_velocity
--     from (
--         select 
--         *
--         from di_crowd.kickboard_session
--         where 
--             exec_dt between '20220901' and '20220930'
--             and ride_yn = 1
--             and service_name not rlike '라임'
--     ) t1
--     join (
--         select 
--         `dec`(ct_gun_gu_cd) as district_id
--         , `dec`(ct_pvc_nm) as do_nm
--         , case when `dec`(ct_pvc_nm) = '세종' then null else split(`dec`(ct_gun_gu_nm), '[\ ]')[0] end as ct_nm
--         , split(`dec`(ct_gun_gu_nm), '[\ ]')[1] as gu_nm
--         , concat( `dec`(ct_pvc_nm), ' ', `dec`(ct_gun_gu_nm) ) AS ctp_sig_name
--         from wind_tmt.mmkt_ldong_cd_d
--         where `dec`(up_myun_dong_nm) = "#"
--             and `dec`(ct_gun_gu_nm) != "#"
--     ) t4
--     on t1.riding_region_code = t4.district_id
--     where t4.do_nm rlike '서울'
-- ) temp 
-- ; 

drop table default.smkim_insight_kickboard_usage_dist ; 
create table default.smkim_insight_kickboard_usage_dist as 

select 
 ct_nm, gubun
 , count(*) as session_cnt 
from (
    select 
    t1.*
    , t4.ct_nm 
    , case 
        when session_distance_km <= 1 then '01_1km 이하'
        when session_distance_km <= 2 then '02_2km 이하'
        when session_distance_km <= 3 then '03_3km 이하'
        when session_distance_km <= 4 then '04_4km 이하'
        when session_distance_km <= 5 then '05_5km 이하'
        when session_distance_km <= 6 then '06_6km 이하'
        when session_distance_km <= 7 then '07_7km 이하'
        when session_distance_km <= 8 then '08_8km 이하'
        when session_distance_km <= 9 then '09_9km 이하'
        when session_distance_km <= 10 then '10_10km 이하'
        else '11_11km 이상' end as gubun  
    from (
        select 
        *
        from di_crowd.kickboard_session
        where 
            exec_dt between '20220901' and '20220930'
            and ride_yn = 1
            and service_name not rlike '라임'
    ) t1
    join (
        select 
        `dec`(ct_gun_gu_cd) as district_id
        , `dec`(ct_pvc_nm) as do_nm
        , case when `dec`(ct_pvc_nm) = '세종' then null else split(`dec`(ct_gun_gu_nm), '[\ ]')[0] end as ct_nm
        , split(`dec`(ct_gun_gu_nm), '[\ ]')[1] as gu_nm
        , concat( `dec`(ct_pvc_nm), ' ', `dec`(ct_gun_gu_nm) ) AS ctp_sig_name
        from wind_tmt.mmkt_ldong_cd_d
        where `dec`(up_myun_dong_nm) = "#"
            and `dec`(ct_gun_gu_nm) != "#"
    ) t4
    on t1.riding_region_code = t4.district_id
    where t4.do_nm rlike '서울'
) temp 
group by ct_nm, gubun 
; 