set hivevar:dt = ${hivevar:dt};
set hive.execution.engine = mr;
set mapreduce.map.memory.mb=10000;
set mapreduce.map.java.opts=-Xmx2819m;
set mapreduce.reduce.memory.mb=20000;
set mapreduce.reduce.java.opts=-Xmx5638m;
set hive.merge.mapredfiles=true;


set hive.execution.engine = mr;
set hive.variable.substitute=true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

insert into table cpm_stg.pf_life_abroad_monthly partition(ym)
select            svc_mgmt_num
,                 airport_loc
,                 out_dt
,                 out_hms
,                 in_dt
,                 in_hms
,                 period
,                 country
,                 duration
,                 ym
from              default.hju_traveler_leave_row_one
where             out_dt ='20230124'
          and     (period > 1 or period is null)
;

-- insert into table cpm_stg.pf_life_abroad_monthly partition(ym)
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
-- where             out_dt ='20230125'
--           and     (period > 1 or period is null)
--           and     country <> '#'
-- ;


-- di_cpm 자산화 종료 

-- insert overwrite table di_cpm.pf_life_abroad_monthly partition (ym)
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
-- where             out_dt <= regexp_replace(date_add(current_date(), -3), '-', '')
--           and     (period > 1 or period is null)
-- ;



