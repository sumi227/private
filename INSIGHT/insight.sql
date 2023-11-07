
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
set hivevar:start_dt = '20220103';
set hivevar:end_dt ='20221231';

drop table default.smkim_insight_apt_daechi_academy_child1 ; 
create table default.smkim_insight_apt_daechi_academy_child1  as 

SELECT 
    t1.feature 
    , representative_yp_id AS yp_id
     , t3.name
     , t3.lat
     , t3.lng
     , count(distinct IF(voc_cnt > 0, t1.svc_mgmt_num, NULL)) AS svc_count
     , sum(call_cnt) AS call_count
    --  , count(distinct IF(call_cnt > 0, svc_mgmt_num, NULL)) AS call_svc_cnt
    --  , count(distinct IF(sms_cnt > 0, svc_mgmt_num, NULL)) AS sms_svc_cnt
    --  , sum(voc_cnt) AS voc_cnt
    --  , sum(sms_cnt) AS sms_cnt
FROM (
    select *
    from smkim_insight_apt_daechi_child_temp
    where feature is not null
) t1 
inner join (
    SELECT exec_ym, svc_mgmt_num, representative_yp_id, representative_category_full_path
    , call_cnt, voc_cnt, sms_cnt, voc_snd_cnt, voc_rcv_cnt, sms_snd_cnt, sms_rcv_cnt
    FROM ${hivevar:db_name}.apt_svc_call_base
    WHERE exec_ym BETWEEN '202201' AND '202210'
          AND representative_category_full_path rlike "교육/학문>학원>영어학원|교육/학문>학원>수학전문학원|교육/학문>학원>입시/고시학원|교육/학문>학원>외국어학원|교육/학문>학원>학원"
) t2 
on t1.svc_mgmt_num = t2.svc_mgmt_num
inner join (
    SELECT yp_id, concat(name1," ", name2) AS name, geo_latitude AS lat, geo_longitude AS lng
    FROM ${hivevar:db_name}.call_meta
    WHERE exec_dt IN (SELECT max(exec_dt) FROM ${hivevar:db_name}.call_meta tb WHERE exec_dt between '20220101' and '20221031' )
          AND rep_yp_id = yp_id -- gp에 있는 경우
          AND rep_yp_del_yn = 'N'
          AND rep_yp_exposure_yn = 'Y' 
          AND geo_latitude IS NOT NULL
          AND geo_longitude IS NOT NULL
) t3 
ON t2.representative_yp_id = t3.yp_id
GROUP BY t1.feature, representative_yp_id, t3.name, t3.lat, t3.lng
 HAVING svc_count > 5
;
