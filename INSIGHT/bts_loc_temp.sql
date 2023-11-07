-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

set hivevar:db_name = di_crowd ;
set hivevar:exec_ym = substr(${hivevar:exec_dt}, 1, 6) ; 
set hivevar:ym_bf1m = substr(regexp_replace(add_months(concat(substr(${hivevar:exec_ym},1,4), '-', substr(${hivevar:exec_ym},5,2), '-01'),-1),'-',''),1,6);

drop table default.diaas_insight_bts_loc_raw_bf  ;
create table default.diaas_insight_bts_loc_raw_bf  as 

with base as (
    select t3.*
    from (
        select t1.* 
        , degrees( acos((sin(radians(t1.lat)) * sin(radians(t2.lat))) +
                        (cos(radians(t1.lat)) * cos(radians(t2.lat)) * cos(radians(t1.lng - t2.lng))))) * 60 * 1.1515 * 1.609344 as home_distance 
        from (
            select * 
            , case when session_status <> 'ride' and session_bld_bgtsn is not null then session_bld_lat
                else latitude end as lat  
            , case when session_status <> 'ride' and session_bld_bgtsn is not null then session_bld_lng
                else longitude end as lng   
            from di_crowd.diaas_loc_base_daily 
            where exec_dt = ${hivevar:exec_dt}
        ) t1
        left join (
            select distinct svc_mgmt_num, nw_latitude_nm as lat, nw_longitude_nm as lng 
            from cpm_stg.pf_svc_home_address_monthly
            where ym = ${hivevar:ym_bf1m} 
        ) t2 
        on t1.svc_mgmt_num = t2.svc_mgmt_num 
    ) t3 
    where home_distance >=0.5
) 


select * 
from (
    select * 
    , substring(loc_tm, 1, 2) as hh 
    , coalesce(lead(second_s) over(partition by svc_mgmt_num order by loc_tm), 86400) - second_s as duration
    from (
        select 
        svc_mgmt_num
        , loc_tm
        , second_s 
        , ldong_cd 
        , gh as gh7 
        , substring(gh, 1, 6) as gh6 
        from (
            select t2.*
            from (
                select *
                , row_number() over ( partition by svc_mgmt_num, loc_tm order by distance ) as rn 
                from
                (
                    select t1.*, gh, gh_y[7] as gh_y7, gh_x[7] as gh_x7
                    , degrees( acos((sin(radians(latitude)) * sin(radians(gh_y[7]))) +
                        (cos(radians(latitude)) * cos(radians(gh_y[7])) * cos(radians(longitude - gh_x[7]))))) * 60 * 1.1515 * 1.609344 as distance 
                    from base t1 
                    join gb27.ryu_p31_gh_lookup2 as t2
                    on floor(t1.longitude/0.002) = floor(t2.gh_x[7]/0.002)
                    and floor(t1.latitude/0.002) = floor(t2.gh_y[7]/0.002)

                    union all

                    select t1.*, gh, gh_y[7] as gh_y7, gh_x[7] as gh_x7
                    , degrees( acos((sin(radians(latitude)) * sin(radians(gh_y[7]))) +
                        (cos(radians(latitude)) * cos(radians(gh_y[7])) * cos(radians(longitude - gh_x[7]))))) * 60 * 1.1515 * 1.609344 as distance 
                    from base t1 
                    join gb27.ryu_p31_gh_lookup2 as t2
                    on floor(t1.longitude/0.002+0.5) = floor(t2.gh_x[7]/0.002+0.5)
                    and floor(t1.latitude/0.002) = floor(t2.gh_y[7]/0.002)

                    union all

                    select t1.*, gh, gh_y[7] as gh_y7, gh_x[7] as gh_x7
                    , degrees( acos((sin(radians(latitude)) * sin(radians(gh_y[7]))) +
                        (cos(radians(latitude)) * cos(radians(gh_y[7])) * cos(radians(longitude - gh_x[7]))))) * 60 * 1.1515 * 1.609344 as distance 
                    from base t1 
                    join gb27.ryu_p31_gh_lookup2 as t2
                    on floor(t1.longitude/0.002) = floor(t2.gh_x[7]/0.002)
                    and floor(t1.latitude/0.002+0.5) = floor(t2.gh_y[7]/0.002+0.5)

                    union all

                    select t1.*, gh, gh_y[7] as gh_y7, gh_x[7] as gh_x7
                    , degrees( acos((sin(radians(latitude)) * sin(radians(gh_y[7]))) +
                        (cos(radians(latitude)) * cos(radians(gh_y[7])) * cos(radians(longitude - gh_x[7]))))) * 60 * 1.1515 * 1.609344 as distance 
                    from base t1 
                    join gb27.ryu_p31_gh_lookup2 as t2
                    on floor(t1.longitude/0.002+0.5) = floor(t2.gh_x[7]/0.002+0.5)
                    and floor(t1.latitude/0.002+0.5) = floor(t2.gh_y[7]/0.002+0.5)
                ) t1 
            ) t2 
            where rn = 1
        ) t3
    ) t4
) t5 
where duration >= 10 * 60
;
    where rn = 1


drop table smkim_insight_bts_loc_summary_bf_gh7;
create table default.smkim_insight_bts_loc_summary_bf_gh7 as 

select a.*, b.sido_nm, b.sgng_nm, b.ldong_nm, b.ldong_cd
from (
select gh7
, substring(loc_tm, 1, 2) as hh 
, count(distinct svc_mgmt_num) as svc_cnt 
from diaas_insight_bts_loc_raw_bf 
group by gh7 , substring(loc_tm, 1, 2) 
) a 
left join (
    select *
from gb27.ryu_p31_gh_lookup  
where gh_res=7
) b  
on a.gh7 = b.gh
 ;
 
 
 drop table smkim_insight_bts_loc_summary_bf_gh6;
create table default.smkim_insight_bts_loc_summary_bf_gh6 as 

select a.*, b.sido_nm, b.sgng_nm, b.ldong_nm, b.ldong_cd
from (
select gh6
, substring(loc_tm, 1, 2) as hh 
, count(distinct svc_mgmt_num) as svc_cnt 
from diaas_insight_bts_loc_raw_bf 
group by gh6 , substring(loc_tm, 1, 2) 
) a 
left join (
    select *
from gb27.ryu_p31_gh_lookup  
where gh_res=6
) b  
on a.gh6 = b.gh
 ;




