
set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;



-- # 전체 방문 lift 

-- drop table default.smkim_insight_poi_visit_ratio ; 
-- create table default.smkim_insight_poi_visit_ratio as

-- select  
--  t1.* 
--  , t2.avg_visit_cnt 
--  , t1.avg_visit_cnt / t2.avg_visit_cnt as visit_ratio 
-- from (
--     select 
--      *
--      , svc_cnt / dt_cnt as avg_visit_cnt  
--     from (
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr 
--         , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
--         , count(distinct exec_dt) as dt_cnt 
--         from di_crowd.poi_hourly_visitors_daily
--         where exec_dt between '20230723' and '20230805'
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr
--     ) t11 
-- ) t1 
-- left join (
--     select 
--      *
--      , svc_cnt / dt_cnt as avg_visit_cnt  
--     from (
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr 
--         , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
--         , count(distinct exec_dt) as dt_cnt 
--         from smkim_diaas_poi_visit_temp
--         where exec_dt between '20230501' and '20230531'
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr
--     ) t21
-- ) t2 
-- on t1.poi_id = t2.poi_id 
-- ;



-- drop table default.smkim_insight_poi_visit_ratio_child ; 
-- create table default.smkim_insight_poi_visit_ratio_child as

-- select  
--  t1.* 
--  , t2.avg_visit_cnt 
--  , t1.avg_visit_cnt / t2.avg_visit_cnt as visit_ratio 
-- from (
--     select 
--      *
--      , svc_cnt / dt_cnt as avg_visit_cnt  
--     from (
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr 
--         , count(distinct visit.exec_dt, visit.svc_mgmt_num) as svc_cnt 
--         , count(distinct visit.exec_dt) as dt_cnt 
--         from (
--             select * 
--             from di_crowd.poi_hourly_visitors_daily
--             where exec_dt between '20230723' and '20230805'
--         ) visit 
--         join (
--             select distinct svc_mgmt_num
--             , 'child' as child_grp
--             from di_cpm.life_stage_parents_child_pred_monthly 
--             where ym between 202305 and 202307 

--             union all 
--             select distinct svc_mgmt_num
--             , 'school' as child_grp
--             from di_cpm.life_stage_school_monthly  
--             where ym between 202305 and 202307 
--                   and feature rlike '초등학생' and feature rlike '부모'
--                   and (cutoff_yn is null or cutoff_yn = 'Y')
--         ) child 
--         on visit.svc_mgmt_num = child.svc_mgmt_num
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr
--     ) t11 
-- ) t1 
-- left join (
--     select 
--      *
--      , svc_cnt / dt_cnt as avg_visit_cnt  
--     from (
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr 
--         , count(distinct visit.exec_dt, visit.svc_mgmt_num) as svc_cnt 
--         , count(distinct visit.exec_dt) as dt_cnt 
--         from (
--             select * 
--             from di_crowd.poi_hourly_visitors_daily
--             where exec_dt between '20230501' and '20230531'
--         ) visit 
--         join (
--             select distinct svc_mgmt_num
--             , 'child' as child_grp
--             from di_cpm.life_stage_parents_child_pred_monthly 
--             where ym between 202303 and 202305

--             union all 
--             select distinct svc_mgmt_num
--             , 'school' as child_grp
--             from di_cpm.life_stage_school_monthly  
--             where ym between 202303 and 202305
--                   and feature rlike '초등학생' and feature rlike '부모'
--                   and (cutoff_yn is null or cutoff_yn = 'Y')
--         ) child 
--         on visit.svc_mgmt_num = child.svc_mgmt_num
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr
--     ) t21
-- ) t2 
-- on t1.poi_id = t2.poi_id 
-- ;



set hivevar:quantile_1=0.0044;
set hivevar:quantile_2=0.0089;
set hivevar:quantile_3=0.0133;
set hivevar:quantile_4=0.0178;
set hivevar:quantile_5=0.0222;
set hivevar:quantile_6=0.0267;
set hivevar:quantile_7=0.0356;
set hivevar:quantile_8=0.0534;
set hivevar:quantile_9=0.0980;


-- drop table default.smkim_poi_summer_per10min_congestion_50 ;
-- create table default.smkim_poi_summer_per10min_congestion_50 as

-- SELECT 
--  poi_id
--  , exec_hh 
--  -- , exec_mm 
--  , avg_visitor_cnt
--  , congestion
--  , CASE
--     WHEN congestion < ${hivevar:quantile_1} THEN 1
--     WHEN congestion < ${hivevar:quantile_2} THEN 2
--     WHEN congestion < ${hivevar:quantile_3} THEN 3
--     WHEN congestion < ${hivevar:quantile_4} THEN 4
--     WHEN congestion < ${hivevar:quantile_5} THEN 5
--     WHEN congestion < ${hivevar:quantile_6} THEN 6
--     WHEN congestion < ${hivevar:quantile_7} THEN 7
--     WHEN congestion < ${hivevar:quantile_8} THEN 8
--     WHEN congestion < ${hivevar:quantile_9} THEN 9
--     ELSE 10
--    END AS congestion_level
-- FROM (
--     select t1.* 
--     , t1.avg_visitor_cnt / p1.total_area_m2 as congestion 
--     from (
--         select poi_id, exec_hh 
--         , avg(approx_visitor_count) as avg_visitor_cnt 
--         from di_crowd.poi_per10minute_congestion 
--         where exec_dt between 20230723 and 20230805 
--         and exec_mm='50'
--         group by poi_id, exec_hh 
--     ) t1 
--     inner join (
--         SELECT poi_id, total_area_m2
--         FROM (
--             SELECT *
--             , ROW_NUMBER() OVER(PARTITION BY poi_id ORDER BY exec_dt DESC) AS rn
--             FROM di_crowd.poi_polygon
--             WHERE exec_dt >= 20230701 --임시로 30일전으로 수정
--                     AND exec_dt <= 20230805
--         ) AS raw_polygon
--         WHERE rn = 1
--     ) p1 
--     ON t1.poi_id = p1.poi_id
-- ) t2 
-- ; 


-- drop table default.smkim_poi_summer_per10min_congestion_stat_50 ;
-- create table default.smkim_poi_summer_per10min_congestion_stat_50 as

-- SELECT 
--  poi_id
--  , exec_hh 
--  -- , exec_mm 
--  , avg_visitor_cnt
--  , congestion
--  , CASE
--     WHEN congestion < ${hivevar:quantile_1} THEN 1
--     WHEN congestion < ${hivevar:quantile_2} THEN 2
--     WHEN congestion < ${hivevar:quantile_3} THEN 3
--     WHEN congestion < ${hivevar:quantile_4} THEN 4
--     WHEN congestion < ${hivevar:quantile_5} THEN 5
--     WHEN congestion < ${hivevar:quantile_6} THEN 6
--     WHEN congestion < ${hivevar:quantile_7} THEN 7
--     WHEN congestion < ${hivevar:quantile_8} THEN 8
--     WHEN congestion < ${hivevar:quantile_9} THEN 9
--     ELSE 10
--    END AS congestion_level
-- FROM (
--     select t1.* 
--     , t1.avg_visitor_cnt / p1.total_area_m2 as congestion 
--     from (
--         select poi_id, exec_hh
--         -- , exec_mm 
--         , avg(approx_visitor_count) as avg_visitor_cnt 
--         from di_crowd.poi_per10minute_congestion 
--         where exec_dt between 20230501 and 20230531
--         and exec_mm='50'
--         group by poi_id, exec_hh
--         -- , exec_mm 
--     ) t1 
--     inner join (
--         SELECT poi_id, total_area_m2
--         FROM (
--             SELECT *
--             , ROW_NUMBER() OVER(PARTITION BY poi_id ORDER BY exec_dt DESC) AS rn
--             FROM di_crowd.poi_polygon
--             WHERE exec_dt  between 20230501 and 20230531
--         ) AS raw_polygon
--         WHERE rn = 1
--     ) p1 
--     ON t1.poi_id = p1.poi_id
-- ) t2 
-- ; 


-- drop table default.smkim_poi_summer_per10min_congestion_summary_50 ; 
-- create table default.smkim_poi_summer_per10min_congestion_summary_50 as

-- select a.poi_id, a.exec_hh
-- -- , a.exec_mm
-- , a.avg_visitor_cnt as visitor_count, a.congestion_adj as congestion 
-- , b.avg_visitor_cnt as visitor_count_stat, b.congestion_adj as congestion_stat
-- from (
--     select *
--     , congestion * 100 as congestion_adj 
--     from smkim_poi_summer_per10min_congestion_50
-- ) a 
-- join (
--     select *
--     , congestion * 100 as congestion_adj 
--     from smkim_poi_summer_per10min_congestion_stat_50
-- ) b 
-- on a.poi_id = b.poi_id 
-- and a.exec_hh = b.exec_hh
-- -- and a.exec_mm = b.exec_mm
-- ;




-- drop table default.smkim_poi_summer_per10min_congestion_stat_3m ;
-- create table default.smkim_poi_summer_per10min_congestion_stat_3m as

-- SELECT 
--  poi_id
--  , exec_hh 
--  -- , exec_mm 
--  , avg_visitor_cnt
--  , congestion
--  , CASE
--     WHEN congestion < ${hivevar:quantile_1} THEN 1
--     WHEN congestion < ${hivevar:quantile_2} THEN 2
--     WHEN congestion < ${hivevar:quantile_3} THEN 3
--     WHEN congestion < ${hivevar:quantile_4} THEN 4
--     WHEN congestion < ${hivevar:quantile_5} THEN 5
--     WHEN congestion < ${hivevar:quantile_6} THEN 6
--     WHEN congestion < ${hivevar:quantile_7} THEN 7
--     WHEN congestion < ${hivevar:quantile_8} THEN 8
--     WHEN congestion < ${hivevar:quantile_9} THEN 9
--     ELSE 10
--    END AS congestion_level
-- FROM (
--     select t1.* 
--     , t1.avg_visitor_cnt / p1.total_area_m2 as congestion 
--     from (
--         select poi_id, exec_hh
--         -- , exec_mm 
--         , avg(approx_visitor_count) as avg_visitor_cnt 
--         from di_crowd.poi_per10minute_congestion 
--         where exec_dt between 20230423 and 20230722
--         and exec_mm='50'
--         group by poi_id, exec_hh
--         -- , exec_mm 
--     ) t1 
--     inner join (
--         SELECT poi_id, total_area_m2
--         FROM (
--             SELECT *
--             , ROW_NUMBER() OVER(PARTITION BY poi_id ORDER BY exec_dt DESC) AS rn
--             FROM di_crowd.poi_polygon
--             WHERE exec_dt  between 20230423 and 20230722
--         ) AS raw_polygon
--         WHERE rn = 1
--     ) p1 
--     ON t1.poi_id = p1.poi_id
-- ) t2 
-- ; 



-- drop table default.smkim_poi_summer_per10min_congestion_summary_3m ; 
-- create table default.smkim_poi_summer_per10min_congestion_summary_3m as

-- select a.poi_id, a.exec_hh
-- -- , a.exec_mm
-- , a.avg_visitor_cnt as visitor_count, a.congestion_adj as congestion 
-- , b.avg_visitor_cnt as visitor_count_stat, b.congestion_adj as congestion_stat
-- from (
--     select *
--     , congestion * 100 as congestion_adj 
--     from smkim_poi_summer_per10min_congestion_50
-- ) a 
-- join (
--     select *
--     , congestion * 100 as congestion_adj 
--     from smkim_poi_summer_per10min_congestion_stat_3m
-- ) b 
-- on a.poi_id = b.poi_id 
-- and a.exec_hh = b.exec_hh
-- -- and a.exec_mm = b.exec_mm
-- ;


drop table default.smkim_insight_poi_summer_visit_lift ; 
create table default.smkim_insight_poi_summer_visit_lift as

select  
 t1.* 
 , t2.visit_rate as visit_rate_bf 
 , t1.visit_rate / t2.visit_rate as lift  
from (
    select distinct 
     poi_id, poi_name, cat1, cat2, cat3, cat4, addr
     , svc_cnt 
     , dt_cnt 
     , svc_cnt / dt_cnt as avg_visit_cnt 
     , (svc_cnt / dt_cnt) / avg_tot_visit_cnt as visit_rate  
    from (
        select *
        , count(distinct exec_dt, svc_mgmt_num) over(partition by poi_id) as svc_cnt 
        , count(distinct exec_dt) over(partition by poi_id) as dt_cnt 
        , count(distinct exec_dt, svc_mgmt_num) over() / count(distinct exec_dt) over() as avg_tot_visit_cnt  
        from di_crowd.poi_hourly_visitors_daily
        where exec_dt between '20230723' and '20230805'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
    ) t11
) t1 
left join (
    select distinct 
     poi_id, poi_name, cat1, cat2, cat3, cat4, addr
     , svc_cnt 
     , dt_cnt 
     , svc_cnt / dt_cnt as avg_visit_cnt 
     , (svc_cnt / dt_cnt) / avg_tot_visit_cnt as visit_rate  
    from (
        select *
        , count(distinct exec_dt, svc_mgmt_num) over(partition by poi_id) as svc_cnt 
        , count(distinct exec_dt) over(partition by poi_id) as dt_cnt 
        , count(distinct exec_dt, svc_mgmt_num) over() / count(distinct exec_dt) over() as avg_tot_visit_cnt  
        from di_crowd.poi_hourly_visitors_daily
        where exec_dt between '20230501' and '20230531'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
    ) t21
) t2 
on t1.poi_id = t2.poi_id 

;


-- # 아이와 함께 방문율의 lift  

drop table default.smkim_insight_poi_summer_visit_lift_child ; 
create table default.smkim_insight_poi_summer_visit_lift_child as

select  
 t1.* 
 , t2.visit_rate as visit_rate_bf 
 , t1.visit_rate / t2.visit_rate as lift  
from (
    select distinct 
     poi_id, poi_name, cat1, cat2, cat3, cat4, addr
     , svc_cnt 
     , dt_cnt 
     , svc_cnt / dt_cnt as avg_visit_cnt 
     , (svc_cnt / dt_cnt) / avg_tot_visit_cnt as visit_rate  
    from (
        select visit.*
        , count(distinct visit.exec_dt, svc_mgmt_num) over(partition by poi_id) as svc_cnt 
        , count(distinct visit.exec_dt) over(partition by poi_id) as dt_cnt 
        , count(distinct visit.exec_dt, visit.svc_mgmt_num) over() / count(distinct visit.exec_dt) over() as avg_tot_visit_cnt  
        from (
            select *
            from di_crowd.poi_hourly_visitors_daily
            where exec_dt between '20230723' and '20230805'
            and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        ) visit
        join (
            select distinct svc_mgmt_num
            , 'child' as child_grp
            from di_cpm.life_stage_parents_child_pred_monthly 
            where ym between 202307 and 202308 

            union all 
            select distinct svc_mgmt_num
            , 'school' as child_grp
            from di_cpm.life_stage_school_monthly  
            where ym between 202307 and 202308 
                  and feature rlike '초등학생' and feature rlike '부모'
                  and (cutoff_yn is null or cutoff_yn = 'Y')
        ) child 
        on visit.svc_mgmt_num = child.svc_mgmt_num
    ) t11
) t1 
left join (
    select distinct 
     poi_id, poi_name, cat1, cat2, cat3, cat4, addr
     , svc_cnt 
     , dt_cnt 
     , svc_cnt / dt_cnt as avg_visit_cnt 
     , (svc_cnt / dt_cnt) / avg_tot_visit_cnt as visit_rate  
    from (
        select visit.*
        , count(distinct visit.exec_dt, svc_mgmt_num) over(partition by poi_id) as svc_cnt 
        , count(distinct visit.exec_dt) over(partition by poi_id) as dt_cnt 
        , count(distinct visit.exec_dt, visit.svc_mgmt_num) over() / count(distinct visit.exec_dt) over() as avg_tot_visit_cnt  
        from (
            select *
            from di_crowd.poi_hourly_visitors_daily
            where exec_dt between '20230501' and '20230531'
            and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        ) visit
        join (
            select distinct svc_mgmt_num
            , 'child' as child_grp
            from di_cpm.life_stage_parents_child_pred_monthly 
            where ym between 202303 and 202305 

            union all 
            select distinct svc_mgmt_num
            , 'school' as child_grp
            from di_cpm.life_stage_school_monthly  
            where ym between 202303 and 202305 
                  and feature rlike '초등학생' and feature rlike '부모'
                  and (cutoff_yn is null or cutoff_yn = 'Y')
        ) child 
        on visit.svc_mgmt_num = child.svc_mgmt_num
    ) t21
) t2 
on t1.poi_id = t2.poi_id 

;

