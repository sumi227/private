--####################################################################################################
--## Project: PUZZLE - Insight Report 
--## Script purpose: Insight Report (주거생활)
--## Date: 2023-01-12
--####################################################################################################

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

set hivevar:dt = ${hivevar:exec_dt};
set hivevar:db_name = di_crowd ;



-- # Target 

-- drop table default.smkim_insight_apt_daechi_svc_list ; 
-- create table default.smkim_insight_apt_daechi_svc_list as

-- select 
--  distinct t1.svc_mgmt_num
--  , t1.kapt_code
--  , t1.kapt_name 
--  , do_nm, ct_nm, b_dng_nm, h_dng_nm
--  , new_zip_num as zip_cd 
--  , svc_count as tot_svc_count 
--  , kaptda_cnt 
-- from (
--     select *
--     from di_crowd.kapt_svc_mapping 
--     where exec_ym = '202212'
-- ) t1 
-- join (
--     select apt_id, svc_count
--     from di_crowd.available_apt_list
--     where 
--         exec_ym = '202212'
-- ) t2 
-- on t1.kapt_code = t2.apt_id 
-- join (
--     select *
--     from di_crowd.diaas_apt_meta 
--     where ct_nm rlike '강남구' and b_dng_nm rlike '대치'
-- ) t3 
-- on t2.apt_id = t3.kapt_code
-- ;


-- drop table default.smkim_insight_apt_daechi_demo ; 
-- create table default.smkim_insight_apt_daechi_demo as 

--     select t1.kapt_code, t1.kapt_name, t2.sex_cd, t2.age_grp, count(distinct t1.svc_mgmt_num) as svc_cnt 
--     from smkim_insight_apt_daechi_svc_list t1
--     join (
--         select 
--         * 
--          , CASE WHEN cast(cust_age_cd AS int) < 20 THEN '10'
--                 WHEN cast(cust_age_cd AS int) < 60 THEN floor(cast(cust_age_cd AS int)/10)*10 
--                 WHEN cast(cust_age_cd AS int) >= 60 THEN '60_over' 
--                 ELSE cust_age_cd END AS age_grp
--         from (
--             select svc_mgmt_num, cust_age_cd, sex_cd
--             , row_number() over(partition by svc_mgmt_num order by dt desc) as rn 
--             from wind.dbm_customer_mst 
--             where dt between '20221201' and '20221231'
--         ) t21 
--         where rn = 1
--     ) t2
--     on t1.svc_mgmt_num = t2.svc_mgmt_num 
--     group by t1.kapt_code, t1.kapt_name, t2.sex_cd, t2.age_grp

-- ;

-- drop table default.smkim_insight_apt_daechi_demo_seoul ; 
-- create table default.smkim_insight_apt_daechi_demo_seoul as 

-- with base as (
--     select 
--     distinct t1.svc_mgmt_num
--     from (
--         select *
--         from di_crowd.kapt_svc_mapping 
--         where exec_ym = '202212'
--     ) t1 
--     join (
--         select apt_id, svc_count
--         from di_crowd.available_apt_list
--         where 
--             exec_ym = '202212'
--     ) t2 
--     on t1.kapt_code = t2.apt_id 
--     join (
--         select *
--         from di_crowd.diaas_apt_meta 
--         where do_nm rlike '서울'
--     ) t3 
--     on t2.apt_id = t3.kapt_code
-- )

-- select t2.sex_cd, t2.age_grp, count(distinct t1.svc_mgmt_num) as svc_cnt 
-- from base t1
-- join (
--         select 
--         * 
--          , CASE WHEN cast(cust_age_cd AS int) < 20 THEN '10'
--                 WHEN cast(cust_age_cd AS int) < 60 THEN floor(cast(cust_age_cd AS int)/10)*10 
--                 WHEN cast(cust_age_cd AS int) >= 60 THEN '60_over' 
--                 ELSE cust_age_cd END AS age_grp
--         from (
--             select svc_mgmt_num, cust_age_cd, sex_cd
--             , row_number() over(partition by svc_mgmt_num order by dt desc) as rn 
--             from wind.dbm_customer_mst 
--             where dt between '20221201' and '20221231'
--         ) t21 
--         where rn = 1
-- ) t2
-- on t1.svc_mgmt_num = t2.svc_mgmt_num 
-- where t2.sex_cd <> '#' and t2.age_grp not rlike 'B'
-- group by t2.sex_cd, t2.age_grp

-- ;

drop table default.smkim_insight_apt_daechi_academy_child2 ; 
create table default.smkim_insight_apt_daechi_academy_child2  as 

SELECT 
    t1.feature_integ 
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
    select *, regexp_replace(feature, '고학년|저학년|부모', '') as feature_integ 
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
GROUP BY t1.feature_integ, representative_yp_id, t3.name, t3.lat, t3.lng
 HAVING svc_count > 5
;
