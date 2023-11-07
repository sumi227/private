
-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

create table if not exists default.smkim_diaas_poi_visit_temp 
(
    svc_mgmt_num    string 
    , exec_hh         string 
    , hday_yn           string 
    , hday_desc         string 
    , duration          int 
    , poi_id            string 
    , poi_name          string 
    , cat1              string 
    , cat2              string 
    , cat3              string 
    , cat4              string 
    , addr              string 
) partitioned by (exec_dt string)
; 

insert overwrite table default.smkim_diaas_poi_visit_temp partition(exec_dt = ${hivevar:exec_dt})

select  
 t1.svc_mgmt_num
 , t1.exec_hh
 , t3.hday_yn 
 , t3.hday_desc
 , max(t1.duration) as duration 
 , t1.poi_id 
 , t2.poi_name 
 , t2.cat1 
 , t2.cat2 
 , t2.cat3 
 , t2.cat4 
 , t2.addr
from (
    select distinct 
     svc_mgmt_num, poi_id, exec_dt, exec_hh
     , max_second_s - min_second_s as duration 
    from di_crowd.poi_per10minute_enb_visitors
    where exec_dt = ${hivevar:exec_dt}
) t1 
join (
    select distinct poi_id, poi_name 
    , concat(rep_lcd_name, ' ', rep_mcd_name) as addr 
    , rep_cat1 as cat1 
    , rep_cat2 as cat2 
    , rep_cat3 as cat3 
    , rep_cat4 as cat4 
    from di_crowd.tmap_rep_poimeta 
    where exec_dt = '20230521' and rep_cat1 <> '교통편의'
) t2 
on t1.poi_id = t2.poi_id 
left join (
    select distinct yyyymmdd as dt, hday_cl_cd as hday_yn, hday_desc
    from tdia_dw.td_hday_info
    where yyyymmdd = ${hivevar:exec_dt}
) t3 
on t1.exec_dt = t3.dt
where t1.duration >= 600
group by 
 t1.svc_mgmt_num
 , t1.exec_hh
 , t3.hday_yn 
 , t3.hday_desc
 , t1.poi_id 
 , t2.poi_name 
 , t2.cat1 
 , t2.cat2 
 , t2.cat3 
 , t2.cat4 
 , t2.addr
;