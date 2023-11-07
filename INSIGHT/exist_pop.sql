-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set hive.tez.container.size = 40960;
set hive.exec.orc.split.strategy = BI;
set hive.support.quoted.identifiers = none ;
set hive.compute.query.using.stats=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.auto.convert.join=false; 

create table if not exists default.smkim_block_exist_pop_daily 
(
  block_code            string
  , adm_code            string
  , hh                  string
  , exist_cnt_skt       bigint 
  , exist_cnt           bigint 
  , work_cnt_skt        bigint
  , work_cnt            bigint
  , active_cnt_skt      bigint
  , active_cnt          bigint
)
partitioned by (dt STRING)
stored as ORC
;

insert overwrite table default.smkim_block_exist_pop_daily partition(dt=${hivevar:dt})

select block_code, adm_code, hh 
, sum(exist_skt_cnt) as exist_cnt
, sum(exist_tot_cnt) as exist_cnt
, sum(work_skt_cnt) as work_cnt_skt
, sum(work_tot_cnt) as work_cnt 
, sum(active_skt_cnt) as active_cnt_skt
, sum(active_tot_cnt) as active_cnt 
from giraf.bloc_exist_pop_cnt 
where dt = ${hivevar:dt}
group by block_code, adm_code, hh 
;