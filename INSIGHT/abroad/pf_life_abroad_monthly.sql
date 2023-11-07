set hivevar:dt = ${hivevar:dt};
set hive.execution.engine = mr;
set mapreduce.map.memory.mb=10000;
set mapreduce.map.java.opts=-Xmx2819m;
set mapreduce.reduce.memory.mb=20000;
set mapreduce.reduce.java.opts=-Xmx5638m;
set hive.merge.mapredfiles=true;

drop table  default.hju_traveler_leave_airport_out_1;
create table  default.hju_traveler_leave_airport_out_1 as
select  t1.svc_mgmt_num
,       substr(default.dec(t1.ldong_cd), 1, 8) as ldong_cd
,       case when substr(default.dec(t1.ldong_cd), 1, 8) in ('11500108', '11500109', '11500111', '28245112') then 'GMP' -- 각각 서울특별시 강서구 공항동, 방화동, 과해동 / 인천광역시 계양구 상야동
              when substr(default.dec(t1.ldong_cd), 1, 8) = '26440102' then 'PUS'
              when substr(default.dec(t1.ldong_cd), 1, 8) = '27140108' then 'TAE'
              when substr(default.dec(t1.ldong_cd), 1, 8) = '42830320' then 'YNY'
              when substr(default.dec(t1.ldong_cd), 1, 8) in ('28110147', '28110152', '28720310')  then 'ICN' -- 각각 인천광역시 중구 운서동, 무의동 / 인천광역시 웅진군 북도면
              when substr(default.dec(t1.ldong_cd), 1, 8) = '50110109' then 'CJU'
              when substr(default.dec(t1.ldong_cd), 1, 8) = '43114250' then 'CJJ'
              when substr(default.dec(t1.ldong_cd), 1, 8) = '46840350' then 'MWX' end as airport_loc
,       t1.dt
,       t1.hour_t
from
(
  select  *
  from    loc.location_points
  where   dt between regexp_replace(date_add(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -96), '-', '')
                 and '${hivevar:dt}'
) as t1
inner join
(
  select  *
  from    data.hju_traveler_leave_min_max
  where   dt between regexp_replace(date_add(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -96), '-', '')
                 and '${hivevar:dt}'
) as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.dt = t2.dt
  and   t1.hour_t = t2.max_hour
where   substr(default.dec(t1.ldong_cd), 1, 8) in ('11500109', '11500108', '11500111', '28245112', '26440102', '27140108', '42830320', '28110147', '28110152', '28720310', '50110109', '43114250', '46840350')
  and   t1.ranking_num in (1, 2, 3)
;




drop table default.hju_travler_leave_airport_out;
create table default.hju_travler_leave_airport_out as
select              tb1.svc_mgmt_num
,                   tb1.airport_loc
,                   tb1.dt as out_dt
,                   tb1.hour_t as out_hour_t
,                   tb2.ym as out_ym
,                   substr(tb1.ldong_cd, 1, 8) as out_code_cd
,                   tb2.hday_n_home_hcode_cd as out_hday_n_home_hcode_cd
,                   tb2.hday_n_work_hcode_cd as out_hday_n_work_hcode_cd
from    default.hju_traveler_leave_airport_out_1 as tb1
left join
(
  select  svc_mgmt_num
  ,       substr(hday_n_home_hcode_cd, 1, 8) as hday_n_home_hcode_cd
  ,       substR(hday_n_work_hcode_cd, 1, 8) as hday_n_work_hcode_cd
  ,       ym
  from    default.sinhee_location_variable_monthly
  where   ym between substr(regexp_replace(add_months(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -4), '-', ''), 1, 6)
    and   substr(regexp_replace(add_months(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -1), '-', ''), 1, 6)
) as tb2
on      trim(tb1.svc_mgmt_num) = trim(tb2.svc_mgmt_num)
  and   substr(regexp_replace(add_months(concat(substr(tb1.dt, 1, 4), '-', substr(tb1.dt, 5, 2), '-', substr(tb1.dt, 7, 2)), -1), '-', ''), 1, 6) = tb2.ym
where   (tb1.ldong_cd != tb2.hday_n_work_hcode_cd and tb1.ldong_cd != tb2.hday_n_home_hcode_cd) -- 전 달의 집 위치와 출국일의 집 위칙 같지 않거나
   or   (tb2.hday_n_work_hcode_cd is null or tb2.hday_n_home_hcode_cd is null) -- 전 달의 집 위치가 추정이 안되는 회선들 포함(전 달에 SKT 사용 안했거나 4G안쓰는 애들 때문)
;


-- 입국 정보(새로)
drop table  default.hju_traveler_leave_airport_in_1;
create table  default.hju_traveler_leave_airport_in_1 as
select  t1.svc_mgmt_num
,       substr(t1.ldong_cd, 1, 8) as ldong_cd
,       t1.dt
,       t1.hour_t
from
(
  select  *
  from    loc.location_points
  where   dt between regexp_replace(date_add(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -96), '-', '')
                 and '${hivevar:dt}'
) as t1
inner join
(
  select  *
  from    data.hju_traveler_leave_min_max
  where   dt between regexp_replace(date_add(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -96), '-', '')
                 and '${hivevar:dt}'
) as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.dt = t2.dt
  and   t1.hour_t = t2.min_hour
where   t1.ranking_num in (1, 2, 3)
;


drop table default.hju_travler_leave_airport_in;
create table default.hju_travler_leave_airport_in as
select              tb1.svc_mgmt_num
,                   tb1.ldong_cd as in_hcode_cd
,                   tb1.dt as in_dt
,                   tb2.ym as in_ym
,                   tb1.hour_t as in_hour_t
,                   tb2.hday_n_home_hcode_cd as in_hday_n_home_hcode_cd
,                   tb2.hday_n_work_hcode_cd as in_hday_n_work_hcode_cd

from    default.hju_traveler_leave_airport_in_1 as tb1
left join
(
  select  svc_mgmt_num
  ,       substr(hday_n_home_hcode_cd, 1, 8) as hday_n_home_hcode_cd
  ,       substR(hday_n_work_hcode_cd, 1, 8) as hday_n_work_hcode_cd
  ,       ym
  from    default.sinhee_location_variable_monthly
  where   ym between substr(regexp_replace(add_months(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -4), '-', ''), 1, 6)
    and   substr(regexp_replace(add_months(from_unixtime(to_unix_timestamp('${hivevar:dt}', 'yyyyMMdd'), 'yyyy-MM-dd'), -1), '-', ''), 1, 6)
) as tb2
on      trim(tb1.svc_mgmt_num) = trim(tb2.svc_mgmt_num)
  and   substr(regexp_replace(add_months(concat(substr(tb1.dt, 1, 4), '-', substr(tb1.dt, 5, 2), '-', substr(tb1.dt, 7, 2)), -1), '-', ''), 1, 6) = tb2.ym
;


drop table default.hju_traveler_leave_airport_out_in;
create table default.hju_traveler_leave_airport_out_in as
select  tbbbb1.svc_mgmt_num
,       tbbbb1.airport_loc
,       tbbbb1.out_dt
,       tbbbb1.out_hour_t as out_hms
,       tbbbb1.in_dt
,       tbbbb1.in_hour_t as in_hms
,       ((datediff(concat(substr(tbbbb1.in_dt, 1, 4), '-', substr(tbbbb1.in_dt, 5, 2), '-', substr(tbbbb1.in_dt, 7, 2)),
                   concat(substr(tbbbb1.out_dt, 1, 4), '-', substr(tbbbb1.out_dt, 5, 2), '-', substr(tbbbb1.out_dt, 7, 2)))) + 1) as period
,       substr(tbbbb1.out_dt, 1, 6) as ym
from
(
  select  tbbb1.*
  from
  (
    select  tbb1.*
    ,       row_number() over(partition by tbb1.svc_mgmt_num, tbb1.airport_loc, tbb1.out_dt, tbb1.out_hour_t order by tbb1.in_dt asc, tbb1.in_hour_t asc) as in_hour_t_num
    ,       row_number() over(partition by tbb1.svc_mgmt_num, tbb1.airport_loc, tbb1.in_dt, tbb1.in_hour_t order by tbb1.out_dt desc, tbb1.out_hour_t desc) as out_hour_t_num
    from
    (
      select    distinct  tb1.*
      from
      (
        select  t1.svc_mgmt_num
        ,       t1.airport_loc
        ,       t1.out_dt
        ,       t1.out_hour_t
        ,       coalesce(t2.in_dt, '99991231') as in_dt
        ,       t2.in_hour_t
        ,       ((datediff(concat(substr(t2.in_dt, 1, 4), '-', substr(t2.in_dt, 5, 2), '-', substr(t2.in_dt, 7, 2)),
                           concat(substr(t1.out_dt, 1, 4), '-', substr(t1.out_dt, 5, 2), '-', substr(t1.out_dt, 7, 2))))*24 +
                           (cast(t2.in_hour_t as int) - cast(t1.out_hour_t as int))) as out_in_diff_time
        from    default.hju_travler_leave_airport_out as t1
        left join  default.hju_travler_leave_airport_in as t2
        on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
      ) as tb1
    ) as tbb1
    where   tbb1.out_dt < tbb1.in_dt
  )as tbbb1
  where   tbbb1.in_hour_t_num = 1
    and   tbbb1.out_hour_t_num = 1
    and   tbbb1.out_in_diff_time > 24 -- 출국 후 귀국 시간 threshold
) as tbbbb1
;


-- 나갔다가 안 들어온 사람들
-- 일단 나간 사람들
drop table default.hju_traveler_leave_out_not_yet_in_1;
create table default.hju_traveler_leave_out_not_yet_in_1 as
select  tb1.*
--,     row_number() over(partition by t1.svc_mgmt_num order by t2.in_dt desc, t2.in_hour_t desc) as in_hour_t_num
--,     row_number() over(partition by t1.svc_mgmt_num order by t1.out_dt desc, t1.out_hour_t desc) as out_hour_t_num
from
(
  select  distinct t1.svc_mgmt_num
  ,       t1.airport_loc
  ,       t1.out_dt
  ,       t1.out_hour_t
  ,       coalesce(t2.in_dt, '99991231') as in_dt
  ,       t2.in_hour_t
  ,       ((datediff(concat(substr(t2.in_dt, 1, 4), '-', substr(t2.in_dt, 5, 2), '-', substr(t2.in_dt, 7, 2)),
          concat(substr(t1.out_dt, 1, 4), '-', substr(t1.out_dt, 5, 2), '-', substr(t1.out_dt, 7, 2))))*24 +
          (cast(t2.in_hour_t as int) - cast(t1.out_hour_t as int))) as out_in_diff_time
  from    default.hju_travler_leave_airport_out as t1
  left join default.hju_travler_leave_airport_in as t2
  on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
) as tb1
--where               tb1.in_dt <= '20171015'
;



-- 나갔다가 안 들어온 사람들
drop table default.hju_traveler_leave_out_not_yet_in_2;
create table default.hju_traveler_leave_out_not_yet_in_2 as
select  *
from    default.hju_traveler_leave_out_not_yet_in_1
where   (out_dt >= in_dt or in_Dt = '99991231')
  and   out_in_diff_time <= 24
;

-- 나갔다가 들어온 사람들
drop table default.hju_traveler_leave_out_not_yet_in_3;
create table default.hju_traveler_leave_out_not_yet_in_3 as
select  t1.*
from    default.hju_traveler_leave_out_not_yet_in_1 as t1
left join default.hju_traveler_leave_out_not_yet_in_2 as t2
on      t1.svc_mgmt_num = t2.svc_mgmt_num
  and   t1.out_dt = t2.out_dt
  and   t1.in_dt = t2.in_dt
where   t2.svc_mgmt_num is null
  and   t2.out_dt is null
  and   t2.in_dt is null
;

drop table default.hju_traveler_leave_out_not_yet_in_4;
create table default.hju_traveler_leave_out_not_yet_in_4 as
select  distinct t1.svc_mgmt_num
,       t1.airport_loc
,       t1.out_dt
,       t1.out_hour_t as out_hms
,       '99991231' as in_dt
,       '99' as in_hms
,       '#' as period
,       substr(t1.out_dt, 1, 6) as ym
from    default.hju_traveler_leave_out_not_yet_in_1 as t1
left join default.hju_traveler_leave_out_not_yet_in_3 as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.out_dt = t2.out_dt
where   t2.svc_mgmt_num is null
  and   t2.out_dt is null
;




-- 위치 정보가 남는 D-93 일 이전에 출국했다가 안 들어온 사람들
drop table default.hju_traveler_leave_out_before_min_dt;
create table default.hju_traveler_leave_out_before_min_dt as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
,       country
,       duration
,       ym
--from              default.hju_pf_life_copy
from    cpm_stg.pf_life_abroad_monthly -- 나중에 다시 열기
where   out_dt <= regexp_replace(date_add(current_date(), -94), '-', '')
        and   in_dt = '99991231'
        and   ym <= '999999'
;


-- 위치 정보가 남는 D-93일 이전에 출국한 사람 중 귀국을 안한 거승로 되어 있었는데, 귀국 했다면 해당 정보를 업데이트
drop table default.hju_traveler_leave_out_before_renew;
create table default.hju_traveler_leave_out_before_renew as
select  tb1.svc_mgmt_num
,       tb1.airport_loc
,       tb1.out_dt
,       tb1.out_hms
,       coalesce(tb2.min_dt, '99991231') as in_dt
,       coalesce(tb2.min_hour_t, '99') as in_hms
,       datediff(concat(substr(tb2.min_dt, 1, 4), '-', substr(tb2.min_dt, 5, 2), '-', substr(tb2.min_dt, 7, 2)),
                 concat(substr(tb1.out_dt, 1, 4), '-', substr(tb1.out_dt, 5, 2), '-', substr(tb1.out_dt, 7, 2))) + 1 as period
--,                 datediff(from_unixtime(unix_timestamp(tb2.min_dt, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(out_dt, 'yyyyMMdd'), 'yyyy-MM-dd')) + 1 as period
,       tb1.country
,       tb1.duration
,       tb1.ym
from    default.hju_traveler_leave_out_before_min_dt as tb1
left join
(
  select  t1.svc_mgmt_num
  ,       regexp_replace(min(t1.dt), '-', '') as min_dt
  ,       regexp_replace(min(t1.hour_t), '-', '') as min_hour_t
  from    loc.location_points as t1
  where   t1.svc_mgmt_num in (select t2.svc_mgmt_num from default.hju_traveler_leave_out_before_min_dt as t2)
    and   t1.dt between date_add(current_date(), -96) and current_date()
  group by t1.svc_mgmt_num
) as tb2
on trim(tb1.svc_mgmt_num) = trim(tb2.svc_mgmt_num)
;





-- 93일 이내에 출국, 귀국 완료한 사람과 출국하고 귀국 안한 사람들을 Union
drop table default.hju_traveler_leave_airport_out_union;
create table default.hju_traveler_leave_airport_out_union as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       cast(out_hms as string) as out_hms
,       in_dt
,       cast(in_hms as string) as in_hms
,       cast(period as string) as period
,       ym
from    default.hju_traveler_leave_airport_out_in
union
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       cast(out_hms as string) as out_hms
,       in_dt
,       cast(in_hms as string) as in_hms
,       cast(period as string) as period
,       ym
from    default.hju_traveler_leave_out_not_yet_in_4
;


-- 94일 전에 출국해서 들어온 사람을 제외하고 모두
drop table default.hju_traveler_leave_airport_out_all;
create table default.hju_traveler_leave_airport_out_all as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       cast(period as string) as period
,       ym
from    default.hju_traveler_leave_out_before_renew
union
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       cast(period as string) as period
,       ym
from    default.hju_traveler_leave_airport_out_union
;




-- 94일 이전에 출국해서 입국까지 마쳤던 사람들
drop table default.hju_traveler_leave_cpm;
create table default.hju_traveler_leave_cpm as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
,       country
,       duration
,       ym
--from              default.hju_pf_life_copy
from    cpm_stg.pf_life_abroad_monthly
where   in_dt != '99991231'
  and   out_dt <= regexp_replace(date_add(current_date(), -94), '-', '')
  and   ym <= '999999'
;


-- 고객 전수 Union
drop table default.hju_traveler_leave_cpm_union_all;
create table default.hju_traveler_leave_cpm_union_all as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
from    default.hju_traveler_leave_cpm
union
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
from    default.hju_traveler_leave_airport_out_all
;

-- 중복 제거
drop table default.hju_traveler_leave_distinct;
create table default.hju_traveler_leave_distinct as
select  distinct svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
from    default.hju_traveler_leave_cpm_union_all
;


drop table  default.hju_traveler_leave_preproc_first_last_1;
create table  default.hju_traveler_leave_preproc_first_last_1 as
select  t1.svc_mgmt_num
,       t1.second_s
,       floor(t1.second_s / 3600) as hour
,       floor(floor(t1.second_s % 3600) / 60) as minute
,       floor(floor(t1.second_s % 3600) % 60) as second
,       t1.dt
,       t1.position_cd
from    default.sinhee_location_preproc_daily as t1
where   1=1
  and   t1.position_cd in ('first', 'last')
  and   t1.dt between regexp_replace(date_add(current_date(), -95), '-', '')
                  and regexp_Replace(current_Date(), '-', '')
;



drop table  default.hju_traveler_leave_preproc_first_last;
create table  default.hju_traveler_leave_preproc_first_last as
select  t1.*
,       concat(if(length(cast(t1.hour as string)) = 1, concat('0', cast(t1.hour as string)), cast(t1.hour as string))
                  ,  if(length(cast(t1.minute as string)) = 1, concat('0', cast(t1.minute as string)), cast(t1.minute as string))
                  ,  if(length(cast(t1.second as string)) = 1, concat('0', cast(t1.second as string)), cast(t1.second as string))) as hms
from    default.hju_traveler_leave_preproc_first_last_1 as t1
where   1=1
  and   t1.svc_mgmt_num in (select t2.svc_mgmt_num from default.hju_traveler_leave_airport_out_all as t2)
;
drop table  default.hju_traveler_leave_preproc_first_last_1;



drop table  default.hju_traveler_leave_out_time;
create table  default.hju_traveler_leave_out_time as
select  t1.svc_mgmt_num
,       t1.airport_loc
,       t1.out_dt
,       coalesce(t2.hms, '#') as out_hms
,       t1.in_dt
,       t1.period
from    default.hju_traveler_leave_distinct as t1
left join default.hju_traveler_leave_preproc_first_last as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.out_dt = t2.dt
  and   t2.position_cd = 'last'
;

drop table  default.hju_traveler_leave_in_time;
create table  default.hju_traveler_leave_in_time as
select  t1.svc_mgmt_num
,       t1.airport_loc
,       t1.out_dt
,       t1.out_hms
,       t1.in_dt
,       coalesce(t2.hms, '#') as in_hms
,       t1.period
from    default.hju_traveler_leave_out_time as t1
left join default.hju_traveler_leave_preproc_first_last as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.in_dt = t2.dt
  and   t2.position_cd = 'first'
;



drop table default.hju_traveler_leave_roaming;
create table default.hju_traveler_leave_roaming as
select  tbbbb1.svc_mgmt_num
,       tbbbb1.airport_loc
,       tbbbb1.out_dt
,       tbbbb1.out_hms
,       tbbbb1.in_dt
,       tbbbb1.in_hms
,       tbbbb1.period
,       concat_ws(',', collect_list(tbbbb1.country_code)) as country
,       concat_ws(',', collect_list(tbbbb1.each_duration)) as duration
,       substr(tbbbb1.out_dt, 1, 6) as ym
from
(
  select  tbbb1.svc_mgmt_num
  ,       tbbb1.airport_loc
  ,       tbbb1.out_dt
  ,       tbbb1.out_hms
  ,       tbbb1.in_dt
  ,       tbbb1.in_hms
  ,       tbbb1.period
  ,       cast(count(tbbb1.call_usag_strt_dt) as string) as each_duration
  ,       tbbb1.country_code
  from
  (
    select  distinct tbb1.svc_mgmt_num
    ,       tbb1.airport_loc
    ,       tbb1.out_dt
    ,       tbb1.out_hms
    ,       tbb1.in_dt
    ,       tbb1.in_hms
    ,       tbb1.period
    ,       tbb2.country_code
    ,       tbb2.call_usag_strt_dt
    ,       tbb1.ym
    from
    (
      select  svc_mgmt_num
      ,       airport_loc
      ,       out_dt
      ,       out_hms
      ,       in_dt
      ,       in_hms
      ,       period
      ,       substr(out_dt, 1, 6) as ym
      from    default.hju_traveler_leave_in_time
    ) as tbb1

    left join
    (
      select  distinct  svc_mgmt_num
      ,       call_usag_strt_dt
      ,       country_code
      ,       dt
      from    loc.ob_roaming_country
      where   1=1
        and   svc_mgmt_num in (select ttt1.svc_mgmt_num from default.hju_traveler_leave_distinct as ttt1)
        and   dt <= regexp_replace(current_date(), '-', '')
        --and dt >= (select min(ttt2.out_dt) from default.hju_traveler_leave_in_time as ttt2)
        and country_code != 'KOR'
    ) as tbb2
    on      trim(tbb1.svc_mgmt_num) = trim(tbb2.svc_mgmt_num)
      and   tbb2.dt <= regexp_replace(current_date(), '-', '')
                    --and tbb2.dt between regexp_replace(date_add(current_date(), -400), '-', '') and regexp_replace(current_date(), '-', '')
    where   tbb2.call_usag_strt_dt between tbb1.out_dt and tbb1.in_dt -- 여기 변경
  ) as tbbb1
  group by tbbb1.svc_mgmt_num
  ,        tbbb1.airport_loc
  ,        tbbb1.out_dt
  ,        tbbb1.out_hms
  ,        tbbb1.in_dt
  ,        tbbb1.in_hms
  ,        tbbb1.period
  ,        tbbb1.country_code
) as tbbbb1
group by tbbbb1.svc_mgmt_num
,        tbbbb1.airport_loc
,        tbbbb1.out_dt
,        tbbbb1.out_hms
,        tbbbb1.in_dt
,        tbbbb1.in_hms
,        tbbbb1.period
;



drop table default.hju_traveler_leave_roaming_null;
create table default.hju_traveler_leave_roaming_null as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       if(period = '#', null, period) as period
,       if(duration = '0', null, country) as country
,       if(duration = '0', null, duration) as duration
,       ym
from    default.hju_traveler_leave_roaming
;





drop table default.hju_traveler_leave_roaming_droped_outs;
create table default.hju_traveler_leave_roaming_droped_outs as
select  t1.svc_mgmt_num
,       t1.airport_loc
,       t1.out_dt
,       t1.out_hms
,       t1.in_dt
,       t1.in_hms
,       t1.period
,       '#' as country
,       '#' as duration
,       substr(t1.out_dt, 1, 6) as ym
from    default.hju_traveler_leave_in_time as t1
left join default.hju_traveler_leave_roaming_null as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.out_dt = t2.out_dt
  and   t1.in_dt = t2.in_dt
  and   t1.period = t2.period
where   t2.out_dt is null
  and   t2.svc_mgmt_num is null
  and   t2.in_dt is null
  and   t2.period is null
;




drop table default.hju_traveler_leave_union_all_last_1;
create table default.hju_traveler_leave_union_all_last_1 as
select  svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       if(period = '#', NULL, period) as period
,       country
--,       if(country = '#', null, country) as country
,       duration
--,       if(duration = '#', null, duration) as duration
,       ym
from    default.hju_traveler_leave_roaming_droped_outs
;



drop table default.hju_traveler_leave_union_all_last;
create table default.hju_traveler_leave_union_all_last as
select  *
from    default.hju_traveler_leave_union_all_last_1
union
select  *
from    default.hju_traveler_leave_roaming_null
;






drop table default.hju_traveler_leave_distinct_last;
create table default.hju_traveler_leave_distinct_last as
select  distinct svc_mgmt_num
,       airport_loc
,       out_dt
,       out_hms
,       in_dt
,       in_hms
,       period
,       country
,       duration
,       ym
from    default.hju_traveler_leave_union_all_last
;




set mapreduce.map.memory.mb=3524;
set mapreduce.map.java.opts=-Xmx2819m;
set mapreduce.reduce.memory.mb=7048;
set mapreduce.reduce.java.opts=-Xmx5638m;
set hive.merge.mapredfiles=true;
drop table default.hju_traveler_leave_row_one_1;
create table default.hju_traveler_leave_row_one_1 as
select  svc_mgmt_num
,       out_dt
,       count(*) as cnt
from    default.hju_traveler_leave_distinct_last
group by svc_mgmt_num
,        out_dt
having   count(*) = 1
;

drop table default.hju_traveler_leave_row_one_2;
create table default.hju_traveler_leave_row_one_2 as
select  svc_mgmt_num
,       out_dt
,       count(*) as cnt
from    default.hju_traveler_leave_distinct_last
group by  svc_mgmt_num
,         out_dt
having    count(*) >= 2
;

drop table  default.hju_traveler_leave_row_one_3;
create table default.hju_traveler_leave_row_one_3 as
select  t1.*
from    default.hju_traveler_leave_distinct_last as t1
join    default.hju_traveler_leave_row_one_2 as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   trim(t1.out_dt) = trim(t2.out_dt)
;

drop table default.hju_traveler_leave_row_one_4;
create table default.hju_traveler_leave_row_one_4 as
select  t1.svc_mgmt_num
,       t1.airport_loc
,       t1.out_dt
,       t1.out_hms
,       t1.in_dt
,       t1.in_hms
,       t1.period
,       t1.country
,       t1.duration
,       t1.ym
from
(
  select  *
  ,       row_number() over(partition by svc_mgmt_num, out_dt order by out_hms desc, in_dt, in_hms, country desc) as row_num
  from    default.hju_traveler_leave_row_one_3
) as t1
where   t1.row_num = 1
;


drop table default.hju_traveler_leave_row_one_5;
create table default.hju_traveler_leave_row_one_5 as
select  t1.*
from    default.hju_traveler_leave_distinct_last as t1
inner join default.hju_traveler_leave_row_one_1 as t2
on      trim(t1.svc_mgmt_num) = trim(t2.svc_mgmt_num)
  and   t1.out_dt = t2.out_dt
;


drop table default.hju_traveler_leave_row_one;
create table default.hju_traveler_leave_row_one as
select  *
from    default.hju_traveler_leave_row_one_5
union
select  *
from    default.hju_traveler_leave_row_one_4
;




set hive.execution.engine = mr;
set hive.variable.substitute=true;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

insert overwrite table cpm_stg.pf_life_abroad_monthly partition(ym)
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
where             out_dt <= regexp_replace(date_add(current_date(), -3), '-', '')
          and     (period > 1 or period is null)
;


di_cpm 자산화 종료 

insert overwrite table di_cpm.pf_life_abroad_monthly partition (ym)
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
where             out_dt <= regexp_replace(date_add(current_date(), -3), '-', '')
          and     (period > 1 or period is null)
;



