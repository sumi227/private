
--####################################################################################################
--## Project: PUZZLE - Insight Report 
--## Script purpose: Insight Report (POI)
--## Date: 2022-12-8
--####################################################################################################

-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = mr;
-- set hive.tez.container.size = 40960;
set hive.exec.orc.split.strategy = BI;
set hive.support.quoted.identifiers = none ;
set hive.compute.query.using.stats=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.auto.convert.join=false; 

set hivevar:dt = ${hivevar:exec_dt};
set hivevar:db_name = di_crowd ;
set hivevar:start_dt = '20220103';
set hivevar:end_dt ='20221231';

-- drop table default.smkim_theme_report_shopping_app_list ; 
-- create table default.smkim_theme_report_shopping_app_list as 

-- select 
--  distinct t1.cat1, t1.cat2, t1.app_title
--  , t1.app_title_eng 
--  , t2.tot_freq
-- from (
--     select cat1, cat2, app_title, app_title_eng
--      , count(distinct ym) over(partition by cat1, cat2, app_title) as app_ym_cnt 
--     from (
--         select distinct cat1
--          , case when app_title_eng rlike 'Welstorymall|Himart' then 'Shopping_etc' else cat2 end as cat2
--          , app_title_eng 
--          , case when app_title_eng = 'gmarket' then 'Gmarket'
--                 when app_title_eng = 'auction_co_kr' then 'Auction' 
--                 when app_title_eng = 'Ticketmonster' then 'TMON' 
--                 when app_title_eng = 'gsshop' then 'GS Shop' 
--                 when app_title_eng = 'interpark_com' then 'Interpark' 
--                 when app_title_eng = 'Himart' then 'LOTTE HIMART' 
--                 when app_title_eng = 'LFmall.co.kr' then 'LFmall' 
--                 when app_title_eng = 'AmorePacificMall' then 'AMORE PACIFIC' 
--                 when app_title_eng = 'uniqlo_kr' then 'UNIQLO KR' 
--                 when app_title_eng = 'feelway_com' then 'Feelway' 
--                 when app_title_eng = 'Welstorymall_com' then 'Welstorymall' 
--                 else regexp_replace(app_title_eng, '_HTTPS|_IPv6', '') end as app_title 
--         , ym 
--         from ats.app_title_new 
--         where ym between '202201' and '202212'
--               and cat1 = 'Shopping'
--     ) temp 
-- ) t1 
-- left join (
--     select cat1, cat2, app_title_eng , sum(host_freq) as tot_freq
--     from di_cpm_etl_dev.app_host_summary_monthly 
--     where ym=202212 
--     group by cat1, cat2, app_title_eng 
-- ) t2 
-- on t1.app_title = t2.app_title_eng 
-- where t1.app_ym_cnt =12
-- ;


drop table default.smkim_theme_report_shopping_duration_weekly_v2_feature_1  ; 
create table default.smkim_theme_report_shopping_duration_weekly_v2_feature_1 as 

select 
 week_num 
 , count(distinct svc_mgmt_num) as tot_unique_svc_cnt  
 , count(distinct poi_id, exec_dt, svc_mgmt_num) as tot_svc_cnt  
 , duration_q1
 , duration_q2
 , duration_q3
 , avg(stay_duration) as avg_duraton
from (
    select t3.*
    , percentile_approx(stay_duration, 0.25) over(partition by week_num) as duration_q1
    , percentile_approx(stay_duration, 0.50) over(partition by week_num) as duration_q2
    , percentile_approx(stay_duration, 0.75) over(partition by week_num) as duration_q3
    from (
        select 
        t1.*
        , w1.week_num 
        from (
            select 
            poi_id, exec_dt, svc_mgmt_num
            , sum(duration) as stay_duration 
            from (
                select *
                , row_number() over(PARTITION BY svc_mgmt_num, poi_id, exec_hh ORDER BY exec_mm desc) AS rn
                from di_crowd.poi_per10minute_visitors 
                where
                    exec_dt between '20220103' and '20220403'
                    and poi_id in (select poi_id from default.jym_to_smkim_poilist )
                    and duration >= 600
            ) temp 
            where rn = 1 
            group by poi_id, exec_dt, svc_mgmt_num, exec_hh
        ) t1 
        left join default.smkim_week_num_2022 w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off t2
        on t1.poi_id = t2.poi_id and t1.exec_dt = t2.day_off 
        where t2.day_off is null 
    ) t3
) t4
group by 
  week_num
  , duration_q1
  , duration_q2
  , duration_q3
;


drop table default.smkim_theme_report_shopping_duration_weekly_v2_feature_2  ; 
create table default.smkim_theme_report_shopping_duration_weekly_v2_feature_2 as 

select 
 week_num 
 , count(distinct svc_mgmt_num) as tot_unique_svc_cnt  
 , count(distinct poi_id, exec_dt, svc_mgmt_num) as tot_svc_cnt  
 , duration_q1
 , duration_q2
 , duration_q3
 , avg(stay_duration) as avg_duraton
from (
    select t3.*
    , percentile_approx(stay_duration, 0.25) over(partition by week_num) as duration_q1
    , percentile_approx(stay_duration, 0.50) over(partition by week_num) as duration_q2
    , percentile_approx(stay_duration, 0.75) over(partition by week_num) as duration_q3
    from (
        select 
        t1.*
        , w1.week_num 
        from (
            select 
            poi_id, exec_dt, svc_mgmt_num
            , sum(duration) as stay_duration 
            from (
                select *
                , row_number() over(PARTITION BY svc_mgmt_num, poi_id, exec_hh ORDER BY exec_mm desc) AS rn
                from di_crowd.poi_per10minute_visitors 
                where
                    exec_dt between '20220404' and '20220703'
                    and poi_id in (select poi_id from default.jym_to_smkim_poilist )
                    and duration >= 600
            ) temp 
            where rn = 1 
            group by poi_id, exec_dt, svc_mgmt_num, exec_hh
        ) t1 
        left join default.smkim_week_num_2022 w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off t2
        on t1.poi_id = t2.poi_id and t1.exec_dt = t2.day_off 
        where t2.day_off is null 
    ) t3
) t4
group by 
  week_num
  , duration_q1
  , duration_q2
  , duration_q3
;

drop table default.smkim_theme_report_shopping_duration_weekly_v2_feature_3  ; 
create table default.smkim_theme_report_shopping_duration_weekly_v2_feature_3 as 

select 
 week_num 
 , count(distinct svc_mgmt_num) as tot_unique_svc_cnt  
 , count(distinct poi_id, exec_dt, svc_mgmt_num) as tot_svc_cnt  
 , duration_q1
 , duration_q2
 , duration_q3
 , avg(stay_duration) as avg_duraton
from (
    select t3.*
    , percentile_approx(stay_duration, 0.25) over(partition by week_num) as duration_q1
    , percentile_approx(stay_duration, 0.50) over(partition by week_num) as duration_q2
    , percentile_approx(stay_duration, 0.75) over(partition by week_num) as duration_q3
    from (
        select 
        t1.*
        , w1.week_num 
        from (
            select 
            poi_id, exec_dt, svc_mgmt_num
            , sum(duration) as stay_duration 
            from (
                select *
                , row_number() over(PARTITION BY svc_mgmt_num, poi_id, exec_hh ORDER BY exec_mm desc) AS rn
                from di_crowd.poi_per10minute_visitors 
                where
                    exec_dt between '20220704' and '20221002'
                    and poi_id in (select poi_id from default.jym_to_smkim_poilist )
                    and duration >= 600
            ) temp 
            where rn = 1 
            group by poi_id, exec_dt, svc_mgmt_num, exec_hh
        ) t1 
        left join default.smkim_week_num_2022 w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off t2
        on t1.poi_id = t2.poi_id and t1.exec_dt = t2.day_off 
        where t2.day_off is null 
    ) t3
) t4
group by 
  week_num
  , duration_q1
  , duration_q2
  , duration_q3
;

drop table default.smkim_theme_report_shopping_duration_weekly_v2_feature_4  ; 
create table default.smkim_theme_report_shopping_duration_weekly_v2_feature_4 as 

select 
 week_num 
 , count(distinct svc_mgmt_num) as tot_unique_svc_cnt  
 , count(distinct poi_id, exec_dt, svc_mgmt_num) as tot_svc_cnt  
 , duration_q1
 , duration_q2
 , duration_q3
 , avg(stay_duration) as avg_duraton
from (
    select t3.*
    , percentile_approx(stay_duration, 0.25) over(partition by week_num) as duration_q1
    , percentile_approx(stay_duration, 0.50) over(partition by week_num) as duration_q2
    , percentile_approx(stay_duration, 0.75) over(partition by week_num) as duration_q3
    from (
        select 
        t1.*
        , w1.week_num 
        from (
            select 
            poi_id, exec_dt, svc_mgmt_num
            , sum(duration) as stay_duration 
            from (
                select *
                , row_number() over(PARTITION BY svc_mgmt_num, poi_id, exec_hh ORDER BY exec_mm desc) AS rn
                from di_crowd.poi_per10minute_visitors 
                where
                    exec_dt between '20221003' and '20221231'
                    and poi_id in (select poi_id from default.jym_to_smkim_poilist )
                    and duration >= 600
            ) temp 
            where rn = 1 
            group by poi_id, exec_dt, svc_mgmt_num, exec_hh
        ) t1 
        left join default.smkim_week_num_2022 w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off t2
        on t1.poi_id = t2.poi_id and t1.exec_dt = t2.day_off 
        where t2.day_off is null 
    ) t3
) t4
group by 
  week_num
  , duration_q1
  , duration_q2
  , duration_q3
;

drop table default.smkim_theme_report_shopping_duration_weekly_v2_feature  ; 
create table default.smkim_theme_report_shopping_duration_weekly_v2_feature as 

select * from smkim_theme_report_shopping_duration_weekly_v2_feature_1
union all 
select * from smkim_theme_report_shopping_duration_weekly_v2_feature_2 
union all 
select * from smkim_theme_report_shopping_duration_weekly_v2_feature_3 
union all 
select * from smkim_theme_report_shopping_duration_weekly_v2_feature_4 
;