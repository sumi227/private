set hivevar:dt = ${hivevar:dt};
set hive.execution.engine = mr;
set mapreduce.map.memory.mb=10000;
set mapreduce.map.java.opts=-Xmx2819m;
set mapreduce.reduce.memory.mb=20000;
set mapreduce.reduce.java.opts=-Xmx5638m;
set hive.merge.mapredfiles=true;

-- drop table  default.smkim_abroad_temp1;
-- create table  default.smkim_abroad_temp1 as
--     select  tbb1.*
--     ,       row_number() over(partition by tbb1.svc_mgmt_num, tbb1.airport_loc, tbb1.out_dt, tbb1.out_hour_t order by tbb1.in_dt asc, tbb1.in_hour_t asc) as in_hour_t_num
--     ,       row_number() over(partition by tbb1.svc_mgmt_num, tbb1.airport_loc, tbb1.in_dt, tbb1.in_hour_t order by tbb1.out_dt desc, tbb1.out_hour_t desc) as out_hour_t_num
--     from
--     (
--       select    distinct  tb1.*
--       from
--       (
--         select  t1.svc_mgmt_num
--         ,       t1.airport_loc
--         ,       t1.out_dt
--         ,       t1.out_hour_t
--         ,       coalesce(t2.in_dt, '99991231') as in_dt
--         ,       t2.in_hour_t
--         ,       ((datediff(concat(substr(t2.in_dt, 1, 4), '-', substr(t2.in_dt, 5, 2), '-', substr(t2.in_dt, 7, 2)),
--                            concat(substr(t1.out_dt, 1, 4), '-', substr(t1.out_dt, 5, 2), '-', substr(t1.out_dt, 7, 2))))*24 +
--                            (cast(t2.in_hour_t as int) - cast(t1.out_hour_t as int))) as out_in_diff_time
--         from    default.hju_travler_leave_airport_out as t1
--         left join  default.hju_travler_leave_airport_in as t2
--         on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
--       ) as tb1
--     ) as tbb1
--     ;

-- drop table  default.smkim_abroad_temp2;
-- create table  default.smkim_abroad_temp2 as


-- select            svc_mgmt_num
-- ,                 airport_loc
-- ,                 out_dt
-- ,                 out_hms
-- ,                 in_dt
-- ,                 in_hms
-- ,                 period
-- ,                 country
-- ,                 duration
-- ,                 ym
-- from              default.hju_traveler_leave_row_one
-- where      (period > 1 or period is null) and (out_dt <= regexp_replace(date_add(current_date(), -3), '-', '') or country <> '#') 
-- ;

drop table  default.smkim_abroad_temp3_2;
create table  default.smkim_abroad_temp3_2 as

    select a.*, b.dt, b.loc_tm as domestic_loc_tm
    from (
    select *
        , unix_timestamp(concat(substr(out_dt, 1, 4), '-', substr(out_dt, 5,2), '-', substr(out_dt, 7,2), ' ', 
                                        substr(out_hms, 1,2), ':', substr(out_hms, 3,2), ':', substr(out_hms, 5,2))) as loc_tm
        , unix_timestamp(concat(substr(out_dt, 1, 4), '-', substr(out_dt, 5,2), '-', substr(out_dt, 7,2), ' ', 
                                        substr(out_hms, 1,2), ':', substr(out_hms, 3,2), ':', substr(out_hms, 5,2))) + 3600 as loc_tm_re
    from hju_traveler_leave_row_one
    where out_dt >= '20230124'
        and country = '#' 
    ) a 
    left outer join (
    select * 
            , unix_timestamp(concat(substr(dt, 1, 4), '-', substr(dt, 5,2), '-', substr(dt, 7,2), ' ', 
                                        substr(loc_tm, 1,2), ':', substr(loc_tm, 3,2), ':', substr(loc_tm, 5,2))) as loc_tm2
    from loc.location_dedup_hourly 
    where dt between '20230124' and '20230125' 
    ) b 
    on a.svc_mgmt_num = b.svc_mgmt_num 
    where b.loc_tm2 > a.loc_tm_re 
; 

-- drop table  default.smkim_abroad_temp4;
-- create table  default.smkim_abroad_temp4 as

-- select 
-- from (
--     select *
--     from smkim_abroad_temp3 
--     where country = '#'
-- ) a
-- left join (
--     select * 
--     from (
        
--         select * 
--             , unix_timestamp(concat(substr(call_usag_strt_dt, 1, 4), '-', substr(call_usag_strt_dt, 5,2), '-', substr(call_usag_strt_dt, 7,2), ' ', 
--                                         substr(call_usag_strt_tm, 1,2), ':', substr(call_usag_strt_tm, 3,2), ':', substr(call_usag_strt_tm, 5,2))) as loc_tm
--         , row_number() over(partition by svc_mgmt_num order by call_usag_strt_dt asc, call_usag_strt_tm asc) as rn 
--         from loc.ob_roaming_country 
--         where dt >='20230120'
--             and country_code not rlike 'KOR'
--     ) temp 
--     where rn = 1
-- ) b
-- on a.svc_mgmt_num = b.svc_mgmt_num 
-- left outer join (
--     select * 
--             , unix_timestamp(concat(substr(dt, 1, 4), '-', substr(dt, 5,2), '-', substr(dt, 7,2), ' ', 
--                                         substr(loc_tm, 1,2), ':', substr(loc_tm, 3,2), ':', substr(loc_tm, 5,2))) as loc_tm2
--     from loc.location_dedup_hourly 
--     where dt >='20230120'
-- ) c
-- on a.svc_mgmt_num = b.svc_mgmt_num 
-- where b.loc_tm2 > a.loc_tm 

-- where b.loc_tm2 > a.loc_tm 
-- ; 


-- create table default.smkim_abroad_final_table_230124 as 
-- select *
-- from hju_traveler_leave_row_one 
-- ;