
set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

set hivevar:dt = ${hivevar:exec_dt};
set hivevar:db_name = di_crowd ;


drop table default.smkim_pr_poi_cafe_temp ; 
create table default.smkim_pr_poi_cafe_temp as 

select t1.poi_id, t1.name_org as poi_name, t1.category 
    , lcd_name 
    , mcd_name 
    , l_scd_name 
    , t2.poi_id, t2.name_org as cafe_name 
    , t2.category as cafe_category 
, degrees( acos((sin(radians(t1.center_wgs84_lat)) * sin(radians(t2.center_wgs84_lat))) +
    (cos(radians(t1.center_wgs84_lat)) * cos(radians(t2.center_wgs84_lat)) * cos(radians(t1.center_wgs84_lon - t2.center_wgs84_lon))))) * 60 * 1.1515 * 1.609344 as loc_distance 
from (
    select distinct poi_id, name_org, center_wgs84_lat , center_wgs84_lon 
    , class_nm_data[0] as category 
    , lcd_name 
    , mcd_name 
    , l_scd_name 
    from tmm_tmap.tmap_poimeta 
    where poi_id in ('187714','1142340','5800430', '535679','182594','41051','366815','206039','189326','558754','1590369'
                    , '395143','187716','394217','187962','1181965','212403','569326','1102430','567192','6801494','8766401'
                    , '2586833','6465926','63607','528242','205065','10205145')
) t1 
left join (
    select distinct poi_id, name_org, center_wgs84_lat , center_wgs84_lon 
    , class_nm_data[0] as category
    from tmm_tmap.tmap_poimeta 
    where class_nm_data[0] rlike '카페'
    and lcd_name rlike '서울|경기'
) t2 
on round(t1.center_wgs84_lat, 1) = round(t2.center_wgs84_lat,1) 
and round(t1.center_wgs84_lon, 1) = round(t2.center_wgs84_lon,1) 
;