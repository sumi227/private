set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;


-- create table if not exists default.smkim_tmap_route_poi
-- (
--     user_key         string 
--     , ticketid       string
--     , req_time       string
--     , depart_name        string
--     , depart_xpos        string
--     , depart_ypos        string
--     , dest_name        string
--     , dest_xpos        string
--     , dest_ypos        string
--     , dest_poiid        string
--     , category_code        string
--     , class_a_name        string
--     , class_b_name        string
--     , class_c_name        string
--     , class_d_name        string
--     , category_name        string
--     , tvas_estimation_time        string
--     , real_estimation_time        string
--     , hh        string
-- ) 
-- partitioned by (exec_dt string)
-- stored as ORC

-- ;

insert overwrite table default.smkim_tmap_route_poi partition(exec_dt=${hivevar:dt})

-- create table default.smkim_tmap_route_poi_temp as 

select  t1.user_key
,       t1.ticketid
,       t1.req_time
,       default.dec(t1.depart_name) as depart_name
,       default.dec(t1.depart_xpos) as depart_xpos
,       default.dec(t1.depart_ypos) as depart_ypos
,       default.dec(t1.dest_name) as dest_name
,       default.dec(t1.dest_xpos) as dest_xpos
,       default.dec(t1.dest_ypos) as dest_ypos
,       t1.dest_poiid
,       t3.category_code
,       t4.class_a_name
,       t4.class_b_name
,       t4.class_c_name
,       t4.class_d_name
,       t4.category_name
,       t1.tvas_estimation_time
,       t1.real_estimation_time
,       t1.hh
from
(
  select  *
  from    tmm_tmap.tmap_routehistory
  where   dt = ${hivevar:dt}
) as t1
left join
(
  select  distinct poi_id
  ,       category_code
  from    tmm_tmap.tmap_poimeta
  where dt=20230907
) as t3
on      t1.dest_poiid = t3.poi_id
left join
(
  select  class_a_name
  ,       class_b_name
  ,       class_c_name
  ,       class_d_name
  ,       category_name
  ,       category_code
  from    tmm_tmap.tmap_poimeta_category
) as t4
on      t3.category_code = t4.category_code
;



