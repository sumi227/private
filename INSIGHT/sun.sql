
--####################################################################################################
--## Project: PUZZLE - Insight Report 
--## Script purpose: Insight Report (유동인구)
--## Date: 2023-01-02
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
-- set hivevar:start_dt = '20221224';
-- set hivevar:end_dt ='20221225';

-- drop table default.smkim_insight_sunrise_home_summary2 ; 
-- create table default.smkim_insight_sunrise_home_summary2 as 

-- select 
--  ct_pvc_nm 
--  , ct_gun_gu_nm
--  , med_distance 
--  , home_addr 
--  , count(distinct svc_mgmt_num) as svc_cnt 
-- from (
--     select 
--     * 
--     , percentile_approx(distance, 0.50) over(partition by ct_pvc_nm, ct_gun_gu_nm) as med_distance
--     from (
--         select 
--         * 
--         , degrees(
--             acos(
--                 (sin(radians(latitude)) * sin(radians(nw_latitude_nm))) +
--                 (cos(radians(latitude)) * cos(radians(nw_latitude_nm)) * cos(radians(longitude - nw_longitude_nm)))
--             )
--         ) * 60 * 1.1515 * 1.609344 as distance
--         from (
--             select t1.* 
--             , t2.nw_latitude_nm
--             , t2.nw_longitude_nm
--             , t2.home_addr 
--             from (
--                 select *
--                 , row_number() over(partition by svc_mgmt_num order by duration desc) as rn 
--                 from di_crowd.travel_loc_stay_session_log_daily 
--                 where exec_dt = '20230101'
--                 and substr(loc_tm, 1,2) ='07'
--                 and duration >=600
--             ) t1 
--             -- # 거주지 제외 
--             left join (
--                 select svc_mgmt_num, dec(lcode_cd) as ldong_cd, dec(ct_nm) as ct_nm
--                 , nw_latitude_nm
--                 , nw_longitude_nm
--                 , concat(dec(do_nm), ' ', dec(ct_nm)) as home_addr 
--                 , row_number() over(partition by svc_mgmt_num order by ym desc) as rn 
--                 from cpm_stg.pf_svc_home_address_monthly
--                 where ym between '202209' and '202212' 
--             ) t2 
--             on t1.svc_mgmt_num = t2.svc_mgmt_num 
--             -- # 직장 제외 
--             left join (
--                 select svc_mgmt_num, dec(hday_n_work_zip_cd) as zip_cd 
--                 from cpm_stg.life_locationfeature_monthly
--                 where ym between '202209' and '202212' 
--             ) t3
--             on t1.svc_mgmt_num = t3.svc_mgmt_num 
--             where 
--             t1.ldong_cd != t2.ldong_cd
--             and t1.zip_cd != t3.zip_cd
--             and t1.ct_gun_gu_nm != t2.ct_nm 
--             and t1.rn = 1 and t2.rn=1
--         ) t4 
--     ) t5 
-- ) tbl 
-- group by 
--  ct_pvc_nm 
--  , ct_gun_gu_nm
--  , med_distance 
--  , home_addr 
-- ; 



drop table default.smkim_insight_sunrise_gun_gu_demo_lift ; 
create table default.smkim_insight_sunrise_gun_gu_demo_lift as 

with tbl as (
    select 
    ct_pvc_nm 
    , ct_gun_gu_nm
    , age_grp 
    , count(distinct exec_dt, svc_mgmt_num) / 0.401 / 9 as avg_svc_cnt 
    from (
        select t1.* 
        from (
            select *
            , CASE WHEN cast(cust_age_cd AS int) < 20 THEN '10'
                    WHEN cast(cust_age_cd AS int) < 60 THEN floor(cast(cust_age_cd AS int)/10)*10 
                    WHEN cast(cust_age_cd AS int) >= 60 THEN '60_over' 
                    ELSE cust_age_cd END AS age_grp
                from di_crowd.travel_loc_stay_session_log_daily 
            where exec_dt in ('20221002','20221009','20221016','20221023','20221030','20221106','20221113','20221120','20221127')
            and substr(loc_tm, 1,2) ='07'
            and duration >=600
        ) t1 
        -- # 거주지 제외 
        left join (
            select svc_mgmt_num, dec(lcode_cd) as ldong_cd 
            from cpm_stg.pf_svc_home_address_monthly
            where ym between '202209' and '202211' 
        ) t2 
        on t1.svc_mgmt_num = t2.svc_mgmt_num 
        -- # 직장 제외 
        left join (
            select svc_mgmt_num, dec(hday_n_work_zip_cd) as zip_cd 
            from cpm_stg.life_locationfeature_monthly
            where ym between '202209' and '202211' 
        ) t3
        on t1.svc_mgmt_num = t3.svc_mgmt_num 
        where 
        t1.ldong_cd != t2.ldong_cd
        and t1.zip_cd != t3.zip_cd
    ) tbl 
    group by 
    ct_pvc_nm 
    , ct_gun_gu_nm
    , age_grp
)


select 
 new.ct_pvc_nm 
 , new.ct_gun_gu_nm
 , new.age_grp 
 , new.approx_visit_cnt as approx_visit_cnt_new
 , bf.avg_svc_cnt as approx_visit_cnt_bf
 , new.approx_visit_cnt / bf.avg_svc_cnt as ratio
from smkim_insight_sunrise_demo2 new
left join tbl bf 
on  new.ct_pvc_nm = bf.ct_pvc_nm
    and new.ct_gun_gu_nm = bf.ct_gun_gu_nm
    and new.age_grp = bf.age_grp
; 