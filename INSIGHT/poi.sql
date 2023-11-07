
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
set hivevar:start_dt = '20220101';
set hivevar:end_dt ='20230205';


-- ######################################## -- 
-- # 2022년 온/오프라인 쇼핑 트렌드   
-- ######################################## -- 


-- # 대상 POI 제한 (모든 기간 존재하는 POI 대상)

-- # 전체 방문자수 
drop table default.smkim_theme_report_shopping_visitor_weekly_2023 ; 
create table default.smkim_theme_report_shopping_visitor_weekly_2023 as 

select 
 week_num
 , visitor_cnt_q1
 , visitor_cnt_q2
 , visitor_cnt_q3
 , sum(approx_visitor_count) as tot_visitor_cnt 
from (
    select 
    t2.*
    , percentile_approx(t2.approx_visitor_count, 0.25) over(partition by t2.week_num) as visitor_cnt_q1
    , percentile_approx(t2.approx_visitor_count, 0.50) over(partition by t2.week_num) as visitor_cnt_q2
    , percentile_approx(t2.approx_visitor_count, 0.75) over(partition by t2.week_num) as visitor_cnt_q3
    from (
        select 
        t1.* 
        , w1.week_num 
        from (
            select *
            from ${hivevar:db_name}.poi_daily_visitor_count 
            where exec_dt between ${hivevar:start_dt} and ${hivevar:end_dt} 
                and poi_id in (select poi_id from default.jym_to_smkim_poilist )
        ) t1 
        left join (
            select * from default.smkim_week_num_2022
            union all 
            select * from default.smkim_week_num_2023
        ) w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off doff 
        on t1.exec_dt = doff.day_off and t1.poi_id = doff.poi_id
        where doff.day_off is null 
    ) t2 
) t3
group by 
 week_num
 , visitor_cnt_q1
 , visitor_cnt_q2
 , visitor_cnt_q3
; 


drop table default.smkim_theme_report_shopping_visitor_weekly_v3_category  ; 
create table default.smkim_theme_report_shopping_visitor_weekly_v3_category as 

select 
 category 
 , week_num
 , visitor_cnt_q1
 , visitor_cnt_q2
 , visitor_cnt_q3
 , sum(approx_visitor_count) as tot_visitor_cnt 
from (
    select 
    t2.*
    , t3.category 
    , percentile_approx(t2.approx_visitor_count, 0.25) over(partition by t3.category, t2.week_num) as visitor_cnt_q1
    , percentile_approx(t2.approx_visitor_count, 0.50) over(partition by t3.category, t2.week_num) as visitor_cnt_q2
    , percentile_approx(t2.approx_visitor_count, 0.75) over(partition by t3.category, t2.week_num) as visitor_cnt_q3
    from (
        select 
        t1.* 
        , w1.week_num 
        from (
            select *
            from ${hivevar:db_name}.poi_daily_visitor_count 
            where exec_dt between ${hivevar:start_dt} and ${hivevar:end_dt} 
                and poi_id in (select poi_id from default.jym_to_smkim_poilist )
        ) t1 
        left join (
            select * from default.smkim_week_num_2022
            union all 
            select * from default.smkim_week_num_2023
        ) w1 
        on t1.exec_dt = w1.cldr_dt 
        left join public.poi_day_off doff 
        on t1.exec_dt = doff.day_off and t1.poi_id = doff.poi_id
        where doff.day_off is null 
    ) t2 
    left join (
        select *
        from (
            select 
            distinct rep_poi_id as poi_id
            , rep_poi_name as poi_name 
            , concat(rep_lcd_name, ' ', rep_mcd_name) as addr 
            , case when rep_cat2 = '아울렛' then '아울렛'
                when rep_cat2 = '복합쇼핑몰' or rep_cat3 ='쇼핑센터' then '복합쇼핑몰'
                when rep_cat3 = '백화점' then '백화점'
                when rep_cat3 = '할인점' then '마트/할인점'
                else '기타' end as category 
            , exec_dt
            , row_number() over(partition by rep_poi_id order by exec_dt desc) as rn 
            from di_crowd.tmap_rep_poimeta 
            where exec_dt between ${hivevar:start_dt} and ${hivevar:end_dt}  
                and rep_cat1 = '쇼핑'
        ) temp 
        where rn = 1 
    ) t3
    on t2.poi_id = t3.poi_id
) t4 
group by 
 category 
 , week_num
 , visitor_cnt_q1
 , visitor_cnt_q2
 , visitor_cnt_q3
; 

-- drop table default.smkim_theme_report_shopping_visitor_weekly_v2_categor  ; 
-- create table default.smkim_theme_report_shopping_visitor_weekly_v2_categor_2023 as 

-- select 
--  category 
--  , week_num
--  , visitor_cnt_q1
--  , visitor_cnt_q2
--  , visitor_cnt_q3
--  , sum(approx_visitor_count) as tot_visitor_cnt 
-- from (
--     select 
--     t2.*
--     , t3.category 
--     , percentile_approx(t2.approx_visitor_count, 0.25) over(partition by t3.category, t2.week_num) as visitor_cnt_q1
--     , percentile_approx(t2.approx_visitor_count, 0.50) over(partition by t3.category, t2.week_num) as visitor_cnt_q2
--     , percentile_approx(t2.approx_visitor_count, 0.75) over(partition by t3.category, t2.week_num) as visitor_cnt_q3
--     from (
--         select 
--         t1.* 
--         , w1.week_num 
--         from (
--             select *
--             from ${hivevar:db_name}.poi_daily_visitor_count 
--             where exec_dt between ${hivevar:start_dt} and ${hivevar:end_dt} 
--                 and poi_id in (select poi_id from default.jym_to_smkim_poilist )
--         ) t1 
--         left join default.smkim_week_num_2023 w1 
--         on t1.exec_dt = w1.cldr_dt 
--         left join public.poi_day_off doff 
--         on t1.exec_dt = doff.day_off and t1.poi_id = doff.poi_id
--         where doff.day_off is null 
--     ) t2 
--     left join (
--         select *
--         from (
--             select 
--             distinct rep_poi_id as poi_id
--             , rep_poi_name as poi_name 
--             , concat(rep_lcd_name, ' ', rep_mcd_name) as addr 
--             , case when rep_cat2 = '아울렛' then '아울렛'
--                 when rep_cat2 = '복합쇼핑몰' or rep_cat3 ='쇼핑센터' then '복합쇼핑몰'
--                 when rep_cat3 = '백화점' then '백화점'
--                 when rep_cat3 = '할인점' then '마트/할인점'
--                 else '기타' end as category 
--             , exec_dt
--             , row_number() over(partition by rep_poi_id order by exec_dt desc) as rn 
--             from di_crowd.tmap_rep_poimeta 
--             where exec_dt between ${hivevar:start_dt} and ${hivevar:end_dt}  
--                 and rep_cat1 = '쇼핑'
--         ) temp 
--         where rn = 1 
--     ) t3
--     on t2.poi_id = t3.poi_id
-- ) t4 
-- group by 
--  category 
--  , week_num
--  , visitor_cnt_q1
--  , visitor_cnt_q2
--  , visitor_cnt_q3
-- ; 
