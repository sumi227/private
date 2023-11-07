-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;


-- # 결제 

-- drop table default.smkim_insight_bts_pay_gh6 ; 
-- create table default.smkim_insight_bts_pay_gh6 as 

-- select 
--  a.gh6 as gh  
--  , gh.y as latitude 
--  , gh.x as longitude
--  , gh.sido_nm 
--  , gh.sgng_nm 
--  , gh.ldong_nm 
--  , a.pay_cnt as pay_cnt_af 
--  , b.pay_cnt as pay_cnt_bf 
--  , a.pay_cnt / b.pay_cnt as rate 
-- from (
--     select gh6 , count(*) as pay_cnt 
--     from di_crowd.pay_offline_gh_svc_daily
--     where exec_dt = '20230617' 
--     group by gh6 
-- ) a 
-- left join (
--     select gh6 , count(*) as pay_cnt 
--     from di_crowd.pay_offline_gh_svc_daily
--     where exec_dt = '20230610' 
--     group by gh6 
-- ) b 
-- on a.gh6 = b.gh6 
-- left join (
--     select *
--     from gb27.ryu_p31_gh_lookup 
--     where gh_res = 6 
-- ) gh 
-- on a.gh6 = gh.gh 
-- ;



-- drop table default.smkim_insight_bts_pay_gh7 ; 
-- create table default.smkim_insight_bts_pay_gh7 as 

-- select 
--  a.gh7 as gh  
--  , gh.y as latitude 
--  , gh.x as longitude
--  , gh.sido_nm 
--  , gh.sgng_nm 
--  , gh.ldong_nm 
--  , a.pay_cnt as pay_cnt_af 
--  , b.pay_cnt as pay_cnt_bf 
--  , a.pay_cnt / b.pay_cnt as rate 
-- from (
--     select gh7 , count(*) as pay_cnt 
--     from di_crowd.pay_offline_gh_svc_daily
--     where exec_dt = '20230617' 
--     group by gh7 
-- ) a 
-- left join (
--     select gh7 , count(*) as pay_cnt 
--     from di_crowd.pay_offline_gh_svc_daily
--     where exec_dt = '20230610' 
--     group by gh7
-- ) b 
-- on a.gh7 = b.gh7
-- left join (
--     select *
--     from gb27.ryu_p31_gh_lookup 
--     where gh_res = 7
-- ) gh 
-- on a.gh7 = gh.gh 
-- ;


-- drop table default.smkim_insight_bts_pay_gh7_hh ;
-- create table default.smkim_insight_bts_pay_gh7_hh as

-- select gh, gh_level, hh, pay_cnt 
-- , pay_cnt/total_count as rate
-- from (
--     select * 
--     , sum(pay_cnt) over(partition by gh ) as total_count 
--     from (
--         select 
--         substring(pay_tm, 1, 2) as hh 
--         , gh7 as gh
--         , 7 as gh_level 
--         , count(distinct svc_mgmt_num, pay_second_s ) as pay_cnt 
--         from  di_crowd.pay_offline_gh_svc_daily 
--         where exec_dt = ${hivevar:exec_dt}
--         group by 
--             substring(pay_tm, 1, 2)
--             , gh7         
--     ) t1
-- ) t2 
-- ;

-- drop table default.smkim_insight_bts_pay_gh7_hh_bf ;
-- create table default.smkim_insight_bts_pay_gh7_hh_bf as

-- select gh, gh_level, hh, pay_cnt 
-- , pay_cnt/total_count as rate
-- from (
--     select * 
--     , sum(pay_cnt) over(partition by gh ) as total_count 
--     from (
--         select 
--         substring(pay_tm, 1, 2) as hh 
--         , gh7 as gh
--         , 7 as gh_level 
--         , count(distinct svc_mgmt_num, pay_second_s ) as pay_cnt 
--         from  di_crowd.pay_offline_gh_svc_daily 
--         where exec_dt = 20230610
--         group by 
--             substring(pay_tm, 1, 2)
--             , gh7         
--     ) t1
-- ) t2 
-- ;



-- drop table default.smkim_insight_bts_loc_gh7_hh ;
-- create table default.smkim_insight_bts_loc_gh7_hh as

-- select gh, gh_level, hh, svc_cnt 
-- , svc_cnt/total_count as rate
-- from (
--     select * 
--     , sum(svc_cnt) over(partition by gh ) as total_count 
--     from (
--         select 
--         substring(loc_tm, 1, 2) as hh 
--         , gh7 as gh
--         , 7 as gh_level 
--         , count(distinct svc_mgmt_num ) as svc_cnt 
--         from  default.diaas_loc_base_geohash_daily
--         where exec_dt = ${hivevar:exec_dt}
--         group by 
--             substring(loc_tm, 1, 2)
--             , gh7         
--     ) t1
-- ) t2 
-- ;


drop table default.smkim_insight_bts_loc_gh7_hh_bf ;
create table default.smkim_insight_bts_loc_gh7_hh_bf as

select gh, gh_level, hh, svc_cnt 
, svc_cnt/total_count as rate
from (
    select * 
    , sum(svc_cnt) over(partition by gh ) as total_count 
    from (
        select 
        substring(loc_tm, 1, 2) as hh 
        , gh7 as gh
        , 7 as gh_level 
        , count(distinct svc_mgmt_num ) as svc_cnt 
        from  default.diaas_loc_base_geohash_daily
        where exec_dt =20230610
        group by 
            substring(loc_tm, 1, 2)
            , gh7         
    ) t1
) t2 
;