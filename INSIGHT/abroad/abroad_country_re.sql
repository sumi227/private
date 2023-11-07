
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
set hivevar:sta_dt = concat(${hivevar:ym}, '01');
set hivevar:end_dt = regexp_replace(date_add(to_Date(add_months(concat(substr(${hivevar:ym},1,4), '-', substr(${hivevar:ym},5,2), '-01'),1)), 15), '-',''); 


drop table default.smkim_abroad_country_valid_temp_2 ; 
create table default.smkim_abroad_country_valid_temp_2 as 

select 
 * 
 , lead(loc_start_tm) over (partition by svc_mgmt_num order by loc_start_tm) - loc_end_tm as tm_diff
    --  , loc_start_tm - lag(loc_end_tm) over (partition by svc_mgmt_num order by loc_start_tm) as tm_diff2
 , row_number() over(partition by svc_mgmt_num, out_dt order by loc_start_tm asc) as rn 
from (
    select 
     svc_mgmt_num
     , out_dt 
     , in_dt 
     , country 
     , duration 
     , country_order_rn
     , country_code       
     , max(loc_tm) - min(loc_tm) as country_duration 
     , min(loc_tm) as loc_start_tm 
     , max(loc_tm) as loc_end_tm 
    from (
        select 
         * 
         , sum(country_counter) over(partition by svc_mgmt_num, out_dt order by rn asc rows between unbounded preceding and current row) as country_order_rn
        from (
            select 
            * 
            , case when country_code = lag(country_code, 1, country_code) over(partition by svc_mgmt_num, out_dt order by rn) then 0 
                    else 1 end as country_counter
            from (
                select 
                t1.svc_mgmt_num
                , t1.out_dt
                , t1.in_dt 
                , t1.country 
                , t1.duration 
                , t2.country_code 
                , t2.call_usag_strt_dt
                , t2.call_usag_strt_tm
                , t2.loc_tm 
                , row_number() over(partition by t1.svc_mgmt_num, t1.out_dt order by t2.loc_tm asc) as rn 
                from (
                    select *
                    from cpm_stg.pf_life_abroad_monthly 
                    where ym = ${hivevar:ym}
                        and airport_loc = 'ICN'
                        and country <> '#'
                        and length(country) >=4
                    distribute by svc_mgmt_num sort by out_dt
                ) t1 
                left outer join (
                    select 
                    * 
                    , unix_timestamp(concat(substr(call_usag_strt_dt, 1, 4), '-', substr(call_usag_strt_dt, 5,2), '-', substr(call_usag_strt_dt, 7,2), ' ', 
                                    substr(call_usag_strt_tm, 1,2), ':', substr(call_usag_strt_tm, 3,2), ':', substr(call_usag_strt_tm, 5,2))) as loc_tm  
                    from   loc.ob_roaming_country 
                    where  dt between ${hivevar:sta_dt} and ${hivevar:end_dt}
                        and country_code != 'KOR'
                    distribute by svc_mgmt_num sort by loc_tm
                ) t2 
                on t1.svc_mgmt_num = t2.svc_mgmt_num
                where t2.call_usag_strt_dt between t1.out_dt and t1.in_dt 
            ) t3 
        ) t4 
    ) t5
    where country_code not rlike 'INO|XA1|XA2'
    group by 
     svc_mgmt_num
     , out_dt 
     , in_dt 
     , country 
     , duration 
     , country_order_rn
     , country_code   
) t6   
; 


-- # =====================================================================
-- # 최종 테이블  # -- 
-- # =====================================================================

create table if not exists default.smkim_life_abroad_monthly
(
    svc_mgmt_num      STRING
,   airport_loc       STRING 
,   out_dt            STRING 
,   out_hms           STRING
,   in_dt             STRING
,   in_hms            STRING
,   period            STRING 
,   country           STRING 
,   duration          STRING
)
partitioned by (exec_ym STRING)
stored as ORC
;



insert overwrite table default.smkim_life_abroad_monthly partition(exec_ym=${hivevar:ym})

select 
 svc_mgmt_num      
 , airport_loc        
 , out_dt             
 , out_hms           
 , in_dt             
 , in_hms            
 , period             
 , country            
 , duration          
from cpm_stg.pf_life_abroad_monthly 
where ym = ${hivevar:ym}
    and airport_loc = 'ICN'
    and length(country) <=3

union all 

select 
 t1.svc_mgmt_num      
 , t1.airport_loc        
 , t1.out_dt             
 , t1.out_hms           
 , t1.in_dt             
 , t1.in_hms            
 , t1.period             
 , t3.country            
 , t3.duration     
from (
    select 
     *
    from cpm_stg.pf_life_abroad_monthly 
    where ym = ${hivevar:ym}
        and airport_loc = 'ICN'
        and length(country) > 3
) t1 
left join (
    select 
     svc_mgmt_num
     , out_dt 
     , in_dt 
     , concat_ws(',', collect_list(country_code)) as country
     , concat_ws(',', collect_list(cast(duration_temp as string))) as duration 
    from (
        select 
        *
        , case when duration is null or duration = 0 then 1 else ceiling(country_duration / 3600 / 24) end as duration_temp 
        from smkim_abroad_country_valid_temp_2 
        where country_code not in ('INO', 'XA1', 'XA2')
            and (tm_diff is null or tm_diff / 3600 >=14 or country_duration >= 3600 * 12)
        distribute by svc_mgmt_num, out_dt sort by rn 
    ) t2 
    group by 
     svc_mgmt_num
     , out_dt 
     , in_dt  
) t3 
on t1.svc_mgmt_num = t3.svc_mgmt_num
   and t1.out_dt = t3.out_dt 
   and t1.in_dt = t3.in_dt 
; 
