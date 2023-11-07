set hivevar:dt = ${hivevar:dt};
set hive.execution.engine = mr;
set mapreduce.map.memory.mb=10000;
set mapreduce.map.java.opts=-Xmx2819m;
set mapreduce.reduce.memory.mb=20000;
set mapreduce.reduce.java.opts=-Xmx5638m;
set hive.merge.mapredfiles=true;

-- drop table default.smkim_abroad_dt_temp ; 
-- create table default.smkim_abroad_dt_temp as

-- select a.svc_mgmt_num , a.out_dt, a.country, a.out_hms, b.country_code, b.call_usag_strt_dt, b.call_usag_strt_tm
-- from (
-- select  *
-- from              default.hju_traveler_leave_row_one
-- where             (period > 1 or period is null)
-- and out_dt = '20230118'
-- and country <>'#'
-- ) a 
-- join (
-- select *
-- from (
-- select *
-- , row_number() over(partition by svc_mgmt_num order by call_usag_strt_dt asc, call_usag_strt_tm asc) as rn 
-- from loc.ob_roaming_country 
-- where dt >='20230118'
-- ) temp 
-- where rn = 1
-- ) b 
-- on a.svc_mgmt_num = b.svc_mgmt_num 
-- ; 


drop table  default.smkim_abroad_temp_tm_diff;
create table  default.smkim_abroad_temp_tm_diff as

select c.svc_mgmt_num, c.out_dt, c.out_hms, c.loc_tm, c.country, c.call_usag_strt_dt, c.call_usag_strt_tm
, min(d.loc_tm2) as min_loc_tm 
, max(d.loc_tm2) as max_loc_tm 
from (
    select a.*, b.call_usag_strt_dt, b.call_usag_strt_tm, b.loc_tm as roaming_tm 
    , row_number() over(partition by a.svc_mgmt_num, a.out_dt order by call_usag_strt_dt asc, call_usag_strt_tm asc) as rn 
    from (
    select *
        , unix_timestamp(concat(substr(out_dt, 1, 4), '-', substr(out_dt, 5,2), '-', substr(out_dt, 7,2), ' ', 
                                        substr(out_hms, 1,2), ':', substr(out_hms, 3,2), ':', substr(out_hms, 5,2))) as loc_tm
    from hju_traveler_leave_row_one
    where out_dt >= '20230123'
        and country <> '#' 
    ) a 
    left outer join (
    select *
        , unix_timestamp(concat(substr(call_usag_strt_dt, 1, 4), '-', substr(call_usag_strt_dt, 5,2), '-', substr(call_usag_strt_dt, 7,2), ' ', 
                                        substr(call_usag_strt_tm, 1,2), ':', substr(call_usag_strt_tm, 3,2), ':', substr(call_usag_strt_tm, 5,2))) as loc_tm
    from loc.ob_roaming_country 
    where dt >='20230123'
    ) b 
    on a.svc_mgmt_num = b.svc_mgmt_num 
    where b.loc_tm > a.loc_tm
    and a.country = b.country_code
) c 
left join (
    select * 
            , unix_timestamp(concat(substr(dt, 1, 4), '-', substr(dt, 5,2), '-', substr(dt, 7,2), ' ', 
                                        substr(loc_tm, 1,2), ':', substr(loc_tm, 3,2), ':', substr(loc_tm, 5,2))) as loc_tm2
    from loc.location_dedup_hourly 
    where dt between '20230123' and '20230124' 
) d
on c.svc_mgmt_num = d.svc_mgmt_num 
where d.loc_tm2 between c.loc_tm and c.roaming_tm 
group by c.svc_mgmt_num, c.out_dt, c.out_hms, c.loc_tm, c.country, c.call_usag_strt_dt, c.call_usag_strt_tm
;