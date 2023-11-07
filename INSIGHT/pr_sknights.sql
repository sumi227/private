-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

-- drop table diaas_kbl_xdr_temp; 
-- create table default.diaas_kbl_xdr_temp as 

-- select domain, request_host , count(*) as log_cnt
-- , count(distinct svc_mgmt_num) as svc_cnt 
-- from di_cpm.xdr_filter_total_raw_daily 
-- where (dt between '20230404' and '20230410' or dt between '20230414' and '20230419') 
-- and domain rlike 'kbl.or'
-- group by domain, request_host 
-- ;

-- drop table default.diaas_kbl_xdr_user; 
-- create table default.diaas_kbl_xdr_user as 

-- select svc_mgmt_num, dt
-- , max(second_s) - min(second_s) as duration
-- , min(second_s) as min_sec
-- , max(second_s) as max_sec
-- , sum(delta_dn_link_data_size) + sum(delta_up_link_data_size) as tot_data_size
-- from (
--     select *
--     , hour* 3600 + minute * 60 + second as second_s 
--     from di_cpm.xdr_filter_total_raw_daily 
--     where dt in ('20230414', '20230418') 
--          and hour between 18 and 21 
--          and domain rlike 'kbl.or'

--     union all 

--     select *
--     , hour* 3600 + minute * 60 + second as second_s 
--     from di_cpm.xdr_filter_total_raw_daily 
--     where dt = '20230416'
--          and hour between 17 and 20
--          and domain rlike 'kbl.or'
-- ) temp 
-- group by svc_mgmt_num, dt
;

drop table default.diaas_kbl_xdr_user_session; 
create table default.diaas_kbl_xdr_user_session as 

select t2.*
from (
    select svc_mgmt_num, domain_session_id, domain_session_duration
    from di_cpm.xdr_filter_domain_session_summary_daily 
    where dt in ('20230414', '20230418', '20230416') 
) t1 
join (
    select *
    from di_cpm.xdr_filter_domain_session_daily 
    where dt in ('20230414', '20230418') 
         and hour between 18 and 21 
         and domain rlike 'kbl.or'

    union all 

    select *
    from di_cpm.xdr_filter_domain_session_daily 
    where dt = '20230416'
         and hour between 17 and 20
         and domain rlike 'kbl.or'
) t2 
on t1.svc_mgmt_num = t2.svc_mgmt_num
and t1.domain_session_id = t2.domain_session_id 