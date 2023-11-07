set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;
set tez.am.container.reuse.enabled=false;

-- drop table default.diaas_insight_tday_rest_meta ; 
-- create  table default.diaas_insight_tday_rest_meta as 

-- select distinct 
--  poi_id, poi_name, cat1, cat2, cat3, cat4, lat, lon, addr, rest_name, gh_7 
-- from (
--     select rest_set.* 
--     , full_set.poi_id_s 
--     , full_set.poi_name_s 
--     , full_set.gh_7 
--     , full_set.distance 
--     from (
--         select distinct poi_id, poi_name, cat1, cat2, cat3, cat4, lat, lon, addr
--         , split(poi_name, '\\[')[0] as rest_name 
--         from di_crowd.diaas_geohash_poi_meta_daily 
--         where exec_dt between 20231002 and 20231003
--         and poi_name rlike '휴게소'
--         and cat3 rlike '고속도로'
--         and cat3 rlike '휴게소'
--     ) rest_set  
--     left outer join (
--         select *
--         from (
--             select *
--             , row_number() over(partition by poi_id_s order by distance asc) as rn 
--             from (
--                 select t1.* 
--                 , t2.poi_id as poi_id_s
--                 , t2.poi_name as poi_name_s
--                 , t2.gh_7 
--                 , degrees(acos((sin(radians(t1.lat)) * sin(radians(t2.lat))) + (cos(radians(t1.lat)) * cos(radians(t2.lat))
--                     * cos(radians(t1.lon - t2.lon))) )) * 60 * 1.1515 * 1.609344 as distance
--                 from (
--                     select distinct poi_id, poi_name, cat1, cat2, cat3, cat4, lat, lon, addr
--                     , split(poi_name, '\\[')[0] as rest_name 
--                     from di_crowd.diaas_geohash_poi_meta_daily 
--                     where exec_dt between 20231002 and 20231003
--                     and poi_name rlike '휴게소'
--                     and cat3 rlike '고속도로'
--                     and cat3 rlike '휴게소'
--                 ) t1 
--                 left outer join (
--                     select distinct poi_id, poi_name, cat1, cat2, lat, lon, addr, gh_7
--                     from di_crowd.diaas_geohash_poi_meta_daily 
--                     where exec_dt between 20231002 and 20231003
--                     and poi_name rlike '휴게소'
--                     and cat1 not rlike '기업|공공|의료|건물|여행'
--                     and cat2 not rlike '마트'
--                     and poi_name not in ('유진휴게소분식','태화휴게소분식')
--                     and poi_name not rlike '임시선별|청송휴게소크리스피|청송휴게소미스터톡톡|석이네식당|동행복권|CU천안성거|대관련휴게소앞|CU동충주|영천댐|호반마을자전거|설창휴게소식당|기사식당|국도주차장|휴게소가든|휴게소한식당|봉암휴게소|장회나루|농공휴게소|휴게소스토리|차령휴게소|백양사|대명휴게소'
--                     and (poi_name not like '%휴게소' or poi_name not like '%휴게소식당' or cat3 rlike '고속도로')
--                 ) t2 
--                 on t1.addr = t2.addr 
--             ) t3 
--         ) temp 
--         where rn = 1
--     ) full_set 
--     on rest_set.rest_name = full_set.rest_name 
--     and rest_set.addr = full_set.addr     
-- ) tbl 
-- ;


-- drop table default.diaas_insight_tday_rest_pay_raw ; 
-- create table default.diaas_insight_tday_rest_pay_raw as

-- select t1.* 
-- , t2.poi_id, t2.poi_name, cat1, cat2, cat3, cat4, lat, lon, addr, rest_name
-- from (
--     select *
--     from di_crowd.pay_offline_gh_svc_daily 
--     where exec_dt between 20230926 and 20231003 
-- ) t1 
-- join diaas_insight_tday_rest_meta t2 
-- on t1.gh7 = t2.gh_7 
-- ;


-- drop table default.diaas_insight_tday_rest_pay_summary ; 
-- create table default.diaas_insight_tday_rest_pay_summary as

-- select poi_id, poi_name, rest_name, cat4 as exp_line, lat, lon, addr
-- , exec_dt
-- , count(distinct svc_mgmt_num) as svc_count 
-- , count(distinct svc_mgmt_num, pay_tm) as pay_count 
-- from diaas_insight_tday_rest_pay_raw 
-- group by poi_id, poi_name, rest_name, cat4, lat, lon, addr, exec_dt



-- drop table default.diaas_insight_tday_rest_pay_summary2 ; 
-- create table default.diaas_insight_tday_rest_pay_summary2 as

-- select move_type, poi_id, poi_name, rest_name, cat4 as exp_line, lat, lon, addr
-- , count(distinct svc_mgmt_num, exec_dt) as svc_count 
-- , count(distinct svc_mgmt_num, exec_dt, pay_tm) as pay_count 
-- from (
--     select * 
--     , case when exec_dt > '20230929' then 'return'
--            when exec_dt ='20230929' and cast(substring(pay_tm, 1, 2) as int) >12 then 'return'
--            else 'go_home' end as move_type 
--     from diaas_insight_tday_rest_pay_raw 
--     where exec_dt >=20230927 
-- ) temp 
-- group by move_type, poi_id, poi_name, rest_name, cat4, lat, lon, addr
-- ;

-- drop table default.diaas_insight_tday_rest_pay_summary_seg ; 
-- create table default.diaas_insight_tday_rest_pay_summary_seg as

-- with seg as (
--     select 
--      pay.* 
--      , case when sex_cd in ('#','B') then 'B' 
--             when sex_cd is null then 'B'
--             else sex_cd end as gender
--      , case when cust_age_cd in ('BBB','###') then 'BBB'
--             when cust_age_cd is null then 'BBB'
--             when cast(cust_age_cd as int) < 100 then floor(cast(cust_age_cd as int)/10)*10 
--             when cast(cust_age_cd as int) >= 100 then '100_over' 
--             else cust_age_cd end as age_grp
--     from (
--         select * 
--         , case when exec_dt > '20230929' then 'return'
--             when exec_dt ='20230929' and cast(substring(pay_tm, 1, 2) as int) >12 then 'return'
--             else 'go_home' end as move_type 
--         from diaas_insight_tday_rest_pay_raw 
--         where exec_dt >=20230927 
--     ) pay
--     inner join (
--         select svc_mgmt_num, sex_cd, cust_age_cd
--         from wind.dbm_customer_mst
--         where dt='20231003' 
--     ) demo 
--     on pay.svc_mgmt_num = demo.svc_mgmt_num
-- )

-- select t1.*
-- , t1.rate / t2.rate as lift 
-- from (
--     select *
--     , pay_count / sum(pay_count) over(partition by move_type, poi_id) as rate 
--     from (
--         select move_type, poi_id, poi_name, rest_name, cat4, lat, lon, addr
--         , gender, age_grp 
--         , count(distinct svc_mgmt_num, exec_dt) as svc_count 
--         , count(distinct svc_mgmt_num, pay_tm ) as pay_count
--         from seg 
--         where gender!='B' and age_grp!='BBB'
--         group by move_type, poi_id, poi_name, rest_name, cat4, lat, lon, addr, gender, age_grp 
--     ) t11 
-- ) t1 
-- left join (
--     select *
--     , pay_count / sum(pay_count) over(partition by move_type) as rate 
--     from (
--         select move_type, gender, age_grp 
--         , count(distinct svc_mgmt_num, exec_dt) as svc_count 
--         , count(distinct svc_mgmt_num, pay_tm ) as pay_count
--         from seg 
--         where gender!='B' and age_grp!='BBB'
--         group by move_type, gender, age_grp  
--     ) t21 
-- ) t2 
-- on t1.move_type = t2.move_type
-- and t1.gender = t2.gender 
-- and t1.age_grp = t2.age_grp 
-- ;



drop table default.diaas_insight_tday_rest_pay_summary_bf ; 
create table default.diaas_insight_tday_rest_pay_summary_bf as

select poi_id, poi_name, rest_name, cat4 as exp_line, lat, lon, addr
, exec_dt
, count(distinct svc_mgmt_num) as svc_count 
, count(distinct svc_mgmt_num, pay_tm) as pay_count 
from (
    select t1.* 
    , t2.poi_id, t2.poi_name, cat1, cat2, cat3, cat4, lat, lon, addr, rest_name
    from (
        select *
        from di_crowd.pay_offline_gh_svc_daily 
        where exec_dt between 20230903 and 20230923
    ) t1 
    join diaas_insight_tday_rest_meta t2 
    on t1.gh7 = t2.gh_7 
) t3 
group by poi_id, poi_name, rest_name, cat4, lat, lon, addr, exec_dt

;
