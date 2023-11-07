
-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

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
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn 
--         , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
--         , count(distinct exec_dt) as dt_cnt 
--         from smkim_diaas_poi_visit_temp
--         where exec_dt between '20230501' and '20230531'
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn
--     ) t11 
-- ) t1 
-- left join (
--     select 
--      *
--      , svc_cnt / dt_cnt as avg_visit_cnt  
--     from (
--         select poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn 
--         , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
--         , count(distinct exec_dt) as dt_cnt 
--         from smkim_diaas_poi_visit_temp
--         where exec_dt between '20230301' and '20230430'
--         group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn
--     ) t21
-- ) t2 
-- on t1.poi_id = t2.poi_id 
--  and t1.hday_yn = t2.hday_yn 
-- ;



drop table default.smkim_insight_poi_visit_lift ; 
create table default.smkim_insight_poi_visit_lift as

select  
 t1.* 
 , t2.visit_rate 
 , t1.visit_rate / t2.visit_rate as lift  
from (
    select 
     t11.*
     , avg_visit_cnt / avg_tot_visit_cnt as visit_rate  
    from (
        select poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn 
        , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
        , count(distinct exec_dt) as dt_cnt 
        , count(distinct exec_dt, svc_mgmt_num) / count(distinct exec_dt) as avg_visit_cnt 
        from smkim_diaas_poi_visit_temp
        where exec_dt between '20230501' and '20230531'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn
    ) t11
    left join (
        select hday_yn 
        , count(distinct exec_dt, svc_mgmt_num) / count(distinct exec_dt) as avg_tot_visit_cnt  
        from smkim_diaas_poi_visit_temp
        where exec_dt between '20230501' and '20230531'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        group by hday_yn
    ) t12
    on t11.hday_yn = t12.hday_yn 
) t1 
left join (
    select 
     t21.*
     , avg_visit_cnt / avg_tot_visit_cnt as visit_rate  
    from (
        select poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn 
        , count(distinct exec_dt, svc_mgmt_num) as svc_cnt 
        , count(distinct exec_dt) as dt_cnt 
        , count(distinct exec_dt, svc_mgmt_num) / count(distinct exec_dt) as avg_visit_cnt 
        from smkim_diaas_poi_visit_temp
        where exec_dt between '20230301' and '20230430'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        group by poi_id, poi_name, cat1, cat2, cat3, cat4, addr, hday_yn
    ) t21
    left join (
        select hday_yn 
        , count(distinct exec_dt, svc_mgmt_num) / count(distinct exec_dt) as avg_tot_visit_cnt  
        from smkim_diaas_poi_visit_temp
        where exec_dt between '20230301' and '20230430'
        and cat1 = '여행/레저' and cat2 not rlike '숙박' and cat3 not rlike '골프|캠핑|운동|체육|스포츠'
        group by hday_yn
    ) t22
    on t21.hday_yn = t22.hday_yn     
) t2 
on t1.poi_id = t2.poi_id 
and t1.hday_yn = t2.hday_yn 

;