--####################################################################################################
--## Project: seg_profile
--## Script purpose: EDA - App/Web 이용 분석
--## Date: 2021-05-01
--####################################################################################################

set hive.execution.engine=tez;
set hive.tez.container.size = 10572;
set tez.grouping.max-size = 515076;
set tez.grouping.min-size = 55076;
set hive.exec.reducers.bytes.per.reducer = 67108864;
set hive.exec.reducers.max = 1009;
set hive.cbo.enable = true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

--# ==================================================================================================
--#  0. Parameter Setting
--# ==================================================================================================

-- drop table default.smkim_week_num ;
-- create table default.smkim_week_num as

-- select
--  t3.week_num
--  , min(t3.cldr_dt) as sta_dt
--  , max(t3.cldr_dt) as end_dt
--  -- , row_number() over() as rn
-- from (
--   select
--    t2.cldr_dt
--    , concat(t2.year, '_', t2.num) as week_num
--   from (
--     select
--      t1.cldr_dt
--      , case when substring(t1.cldr_dt, 5,2) = '01' and t1.num > 50 then cast(substring(t1.cldr_dt, 1, 4) as int) - 1
--             when substring(t1.cldr_dt, 5,2) = '12' and t1.num = 1 then cast(substring(t1.cldr_dt, 1, 4) as int)  + 1
--             else substring(t1.cldr_dt, 1, 4) end as year
--      , case when t1.num < 10 then concat('0', t1.num) else cast(t1.num as string) end as num
--     from (
--       select *
--       , weekofyear(concat(substring(cldr_dt, 1, 4), '-', substring(cldr_dt, 5, 2), '-', substring(cldr_dt, 7,2))) as num
--       from  wind.td_zngm_cldr
--       where cldr_cd = 'CUTDY'
--     ) t1
--   ) t2
-- ) t3
-- group by t3.week_num
-- ;


-- drop table default.smkim_week_num_2022 ;
-- create table default.smkim_week_num_2022 as


--   select
--    t2.cldr_dt
--    , concat(t2.year, '_', t2.num) as week_num
--   from (
--     select
--      t1.cldr_dt
--      , case when substring(t1.cldr_dt, 5,2) = '01' and t1.num > 50 then cast(substring(t1.cldr_dt, 1, 4) as int) - 1
--             when substring(t1.cldr_dt, 5,2) = '12' and t1.num = 1 then cast(substring(t1.cldr_dt, 1, 4) as int)  + 1
--             else substring(t1.cldr_dt, 1, 4) end as year
--      , case when t1.num < 10 then concat('0', t1.num) else cast(t1.num as string) end as num
--     from (
--       select *
--       , weekofyear(concat(substring(cldr_dt, 1, 4), '-', substring(cldr_dt, 5, 2), '-', substring(cldr_dt, 7,2))) as num
--       from  wind.td_zngm_cldr
--       where cldr_cd = 'CUTDY'
--       and cldr_dt between '20220101' and '20221231'
--     ) t1
--   ) t2
-- ;


insert into table default.smkim_child_inst_etc_eng_kinder

select distinct t1.name1, t1.norm_phone_number, t1.road_addr, t1.road_etc_addr, t1.geo_latitude, t1.geo_longitude
from (
    select *
    from t114.tb_yp
    where 
      name1 rlike '몬테키즈' 
      and name1 not rlike '글로티스|탈렌티드|영어전문학원|영어학원|라이즈잉글리시|킹스키즈유치원|라이즈업|어린이집|젭스|국제학교' 
      and del_yn = 'N'
) t1 
join (
    select category_id, yp_id 
    from t114.tb_category_yp_rel
    where category_id in ('8366', '8371','8384', '8417', '8418', '8427', '8437', '8458', '8466','8604', '9381')
) t2 
on t1.yp_id = t2.yp_id 
left join smkim_child_inst_etc_eng_kinder t3
on t1.norm_phone_number = t3.norm_phone_number
where t3.norm_phone_number is null 
;