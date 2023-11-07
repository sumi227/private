
--####################################################################################################
--## Project: PUZZLE - Insight Report 
--## Script purpose: Insight Report (유동인구)
--## Date: 2023-01-02
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


-- set hivevar:dt = ${hivevar:exec_dt};
-- set hivevar:db_name = di_crowd ;
-- -- set hivevar:start_dt = '20221001';
-- -- set hivevar:end_dt ='20221231';

-- # =====================================================================
-- # BLOC_EXIST_POP_CNT  # -- 
-- # =====================================================================

-- drop table default.smkim_block_cd_centroid_list ; 
-- create table default.smkim_block_cd_centroid_list as 

-- select dec(block_code) as block_code, centroidx, centroidy, count(*) as cnt 
-- from giraf.bloc_exist_pop_cnt  
-- where 
--  dt between ${hivevar:start_dt} and ${hivevar:end_dt}
-- group by dec(block_code), centroidx, centroidy
-- ;



-- drop table default.smkim_bloc_exist_cell_cnt_check ; 
-- create table default.smkim_bloc_exist_cell_cnt_check as 

-- select dec(a.adm_code) as adm_code 
-- , b.addr
-- , dec(a.home_adm_code) as home_adm_code
-- , c.addr as home_addr 
-- , tot_exist_skt_cnt, tot_home_skt_cnt, tot_work_skt_cnt,tot_active_skt_cnt, tot_cnt, gap, dt, hh
-- from (
-- select dt, hh, adm_code, home_adm_code, sum(exist_skt_cnt ) as tot_exist_skt_cnt
-- , sum(home_skt_cnt ) as tot_home_skt_cnt 
-- , sum(work_skt_cnt ) as tot_work_skt_cnt 
-- , sum(active_skt_cnt ) as tot_active_skt_cnt
-- , sum(home_skt_cnt ) + sum(work_skt_cnt ) + sum(active_skt_cnt ) as tot_cnt
-- , sum(exist_skt_cnt )- sum(home_skt_cnt ) - sum(work_skt_cnt ) - sum(active_skt_cnt ) as gap
-- from giraf.bloc_exist_cell_cnt 
-- where dt ='20230114'
-- 	and adm_code in (
-- 		select rdong_cd
-- 		from wind.bd_zngm_rdong
-- 		where dec(ct_gun_gu_nm ) rlike '강남구' and dec(ct_pvc_nm ) = '서울'
-- 		and dec(up_myun_dong_nm ) = '역삼1동'
-- 	)
-- 	and hh ='18'
-- group by dt, hh, adm_code, home_adm_code
-- ) a 
-- left join (
-- select rdong_cd, concat(dec(ct_pvc_nm ),' ', dec(ct_gun_gu_nm ),' ', dec(up_myun_dong_nm )) as addr 
-- from wind.bd_zngm_rdong
-- where dec(ct_gun_gu_nm ) rlike '강남구' and dec(ct_pvc_nm ) = '서울'
-- ) b 
-- on dec(a.adm_code) = dec(b.rdong_cd)
-- left join (
-- select rdong_cd, concat(dec(ct_pvc_nm ),' ', dec(ct_gun_gu_nm ),' ', dec(up_myun_dong_nm )) as addr 
-- from wind.bd_zngm_rdong
-- ) c
-- on dec(a.home_adm_code) = dec(c.rdong_cd)
-- ;
-- ;



-- drop table default.smkim_bloc_inflow_cell_temp  ; 
-- create table default.smkim_bloc_inflow_cell_temp as 

-- select a.home_adm_code, c.addr, a.c_uid, a.sector, city, gu, dong
-- , tot_skt_cnt
-- , tot_skt_active
-- from (
-- select dt, hh, home_adm_code, c_uid, sector
-- , sum(skt_cnt ) as tot_skt_cnt 
-- , sum(skt_active ) as tot_skt_active
-- from giraf.bloc_inflow_cell_cnt 
-- where dt ='20230114'
-- 	-- and dec(home_adm_code) = '1168064000'
-- 	and hh ='18'
-- group by dt, hh, home_adm_code, c_uid, sector
-- ) a 
-- left join (
-- select distinct c_uid, mme_grp_id, enb_id, bts_name , region_id, vendor_id
-- , city, gu, dong, bungi
-- from cms_raw.enb 
-- ) b 
-- on a.c_uid = b.c_uid 
-- left join (
-- select rdong_cd, concat(dec(ct_pvc_nm ),' ', dec(ct_gun_gu_nm ),' ', dec(up_myun_dong_nm )) as addr 
-- from wind.bd_zngm_rdong
-- ) c
-- on a.home_adm_code = c.rdong_cd 
-- ;

-- drop table default.smkim_bloc_exist_cell_temp  ; 
-- create table default.smkim_bloc_exist_cell_temp as 

-- select a.home_adm_code, c.addr, a.c_uid, a.cell_id, city, gu, dong
-- , tot_exist_skt_cnt
-- , tot_home_skt_cnt
-- , tot_work_skt_cnt
-- , tot_active_skt_cnt
-- from (
-- select dt, hh, home_adm_code, c_uid, cell_id 
-- , sum(exist_skt_cnt ) as tot_exist_skt_cnt
-- , sum(home_skt_cnt ) as tot_home_skt_cnt 
-- , sum(work_skt_cnt ) as tot_work_skt_cnt 
-- , sum(active_skt_cnt ) as tot_active_skt_cnt
-- from giraf.bloc_exist_cell_cnt 
-- where dt ='20230114'
-- --	and dec(adm_code) ='1168064000'
-- 	-- and dec(home_adm_code )='1168064000'
-- 	and hh ='18'
-- group by dt, hh, home_adm_code, c_uid, cell_id 
-- ) a 
-- left join (
-- select distinct c_uid, mme_grp_id, enb_id, bts_name , region_id, vendor_id
-- , city, gu, dong, bungi
-- from cms_raw.enb 
-- ) b 
-- on a.c_uid = b.c_uid 
-- left join (
-- select rdong_cd, concat(dec(ct_pvc_nm ),' ', dec(ct_gun_gu_nm ),' ', dec(up_myun_dong_nm )) as addr 
-- from wind.bd_zngm_rdong
-- ) c
-- on a.home_adm_code = c.rdong_cd 

-- ;
-- drop table default.smkim_subway_report_sadang_block_temp ; 
-- create table default.smkim_subway_report_sadang_block_temp as 

--     select * 
--     , '사당역' as station 
--     from default.rto_so_area_geo 
--     where ldong_nm rlike '사당|방배|남현' and sido_nm rlike '서울'
--     ;


-- drop table default.smkim_subway_report_sadang_block_list ; 
-- create table default.smkim_subway_report_sadang_block_list as 

-- select *
-- , degrees(
--     acos(
--         (sin(radians(centroidx)) * sin(radians(126.98161))) +
--         (cos(radians(centroidx)) * cos(radians(126.98161)) * cos(radians(centroidy - 37.47653)))
--     )
--   ) * 60 * 1.1515 * 1.609344 as distance
-- from default.rto_so_area_geo 
-- where ldong_nm rlike '사당|방배|남현' and sido_nm rlike '서울'

-- drop table default.smkim_subway_report_sadang_block_exit_list ; 
-- create table default.smkim_subway_report_sadang_block_exit_list as 


-- select *
-- , row_number() over(partition by block_cd order by exit_distance asc) as rn 
-- from (
--     select * 
--     , degrees(
--         acos(
--             (sin(radians(centroidx)) * sin(radians(rep_poi_lon))) +
--             (cos(radians(centroidx)) * cos(radians(rep_poi_lon)) * cos(radians(centroidy - rep_poi_lat)))
--         )
--     ) * 60 * 1.1515 * 1.609344 as exit_distance
--     from (
--         select 
--         t1.* 
--         , t2.rep_poi_lat 
--         , t2.rep_poi_lon
--         from (
--             SELECT t1.*, cast(tb.exit as string) as exit
--             FROM (
--                 SELECT *
--                 FROM smkim_subway_report_sadang_block_list
--             ) as t1
--             LATERAL VIEW explode(array('1','2','3','4','5','6','7','8','9','10','11','12','13','14')) tb as exit
--         ) t1
--         left join (
--             select *  
--             from di_crowd.diaas_subway_station_exit_meta 
--             where exec_dt = '20230205'
--                 and station = '사당역'
--         ) t2 
--         on t1.exit = t2.exit    
--     ) t3 
-- ) t4 

-- ;

drop table default.smkim_subway_report_sadang_block_exist_pop; 
create table default.smkim_subway_report_sadang_block_exist_pop as 

select block_cd, hday_yn, day_desc, hh
, avg(exist_cnt) as avg_exist_cnt
, avg(work_cnt) as avg_work_cnt 
, avg(active_cnt) as avg_active_cnt 
from (
    select t1.* 
    , t2.hday_cl_cd as hday_yn 
    , t2.hday_desc as day_desc 
    from (
        select dt, hh, dec(block_code) as block_cd
        , sum(exist_tot_cnt) as exist_cnt
        , sum(work_tot_cnt) as work_cnt 
        , sum(active_tot_cnt) as active_cnt 
        from giraf.bloc_exist_pop_cnt 
        where dt between '20230101' and '20230131'
        and dec(block_code) in (select distinct block_cd 
                        from smkim_subway_report_sadang_block_exit_list 
                        where distance <=0.5 and rn=1 and exit != '3'
                        )
        group by dt, hh, dec(block_code)  
    ) t1 
    left join tdia_dw.td_hday_info t2
    on t1.dt = t2.yyyymmdd   
) t3 
group by block_cd, hday_yn, day_desc, hh
; 