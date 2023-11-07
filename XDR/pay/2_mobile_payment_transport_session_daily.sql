-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set hive.tez.container.size = 10572;
set hive.exec.orc.split.strategy = BI;
set hive.support.quoted.identifiers = none ;
set hive.auto.convert.join = true;
set hive.compute.query.using.stats=true;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;


-- 기준일자
set hivevar:dt = ${hivevar:dt};
set hivevar:ym = substring(${hivevar:dt}, 1,6) ; 

-- tables
set hivevar:raw_tbl = di_cpm.xdr_filter_total_raw_daily ;
set hivevar:tb_app_title_change = di_cpm_etl_dev.ats_app_title_monitoring_monthly;

-- # =====================================================================
-- #  테이블 정의
-- # =====================================================================

 create table if not exists di_cpm_etl_dev.mobile_payment_transport_session_daily
 (
  svc_mgmt_num string
  ,sex_cd string
  ,cust_age_cd string
  ,source string
  ,app_id string
  ,protocol string
  ,app_title string
  ,app_title_eng string
  ,app_title_ko string
  ,app_group_cd string
  ,cat1 string
  ,cat2 string
  ,request_host string
  ,domain string
  ,domain_original string
  ,subdomain string
  ,domid string
  ,suffix string
  ,request_referer string
  ,request_referer_domid string
  ,hour string
  ,minute string
  ,second string
  ,request_time string
  ,sec int
  ,app_session_id string
  ,app_session_duration int
  ,delta_up_link_data_size double
  ,delta_dn_link_data_size double
 )
 partitioned by (dt string)
 stored as ORC
 ;

-- # =====================================================================
-- #  최종테이블 파티션 적재
-- # =====================================================================

insert overwrite table di_cpm_etl_dev.mobile_payment_transport_session_daily partition(dt=${hivevar:dt})

  select
    t1.svc_mgmt_num
    ,t1.sex_cd
    ,t1.cust_age_cd
    ,t1.source
    ,t1.app_id
    ,t1.protocol
    ,t1.app_title
    ,t1.app_title_eng
    ,t1.app_title_ko
    ,t1.app_group_cd
    ,t1.cat1
    ,t1.cat2
    ,t1.request_host
    ,t1.domain -- 일부 domain은 수동으로 변경
    ,t1.domain_original
    ,t1.subdomain
    ,t1.domid
    ,t1.suffix
    ,t1.request_referer
    ,t1.request_referer_domid
    ,t1.hour
    ,t1.minute
    ,t1.second
    ,t1.request_time
    ,t1.sec
    ,concat(t1.app_title, '_', t1.app_s_counter) as app_session_id
    ,t1.app_session_duration
    ,t1.delta_up_link_data_size
    ,t1.delta_dn_link_data_size
    from
    ( -- t1
      select
        t2.*
        ,dense_rank() over(partition by svc_mgmt_num, app_title order by app_s_sum) as app_s_counter
      from
      ( -- t2
        select
          t3.*
          ,concat(app_title,'_',app_s_sum) as app_session_id
          ,max(sec) over(partition by concat(svc_mgmt_num,'_',${hivevar:dt},'_',app_title,'_',app_s_sum))
            - min(sec) over(partition by concat(svc_mgmt_num,'_',${hivevar:dt},'_',app_title,'_',app_s_sum)) as app_session_duration
        from
        ( -- t3
          select
            t4.*
            ,sum(app_counter) over(partition by svc_mgmt_num, app_title order by rn asc rows between unbounded preceding and current row) as app_s_sum
          from
          ( -- t4
            select
              t5.*
          ,case when app_timediff > 3600 then 1
            else 0 end as app_counter
            from
            ( -- t5
              select
                t6.*
                ,sec - lag(sec, 1, sec) over(partition by svc_mgmt_num, app_title order by rn) as app_timediff
              from
              ( -- t6: 시간순 row number 추가
                select
                  t7.*
                  ,row_number() over(partition by svc_mgmt_num order by sec) as rn
                from
                ( -- t7: transport
                    select *
                      ,3600*cast(hour as int) + 60*cast(minute as int) + cast(second as int) as sec
                    from ${hivevar:raw_tbl}
                    where dt=${hivevar:dt}
                      and app_title in (
                        select case when b.af_app_title_eng is not null then b.af_app_title_eng else a.app_title_eng end as app_title
                        from (
                            select distinct app_title_eng 
                            from ats.app_title_new 
                            where ym = ${hivevar:ym}
                            and cat2 rlike 'Transport|sharing|Ride_request'
                            and app_title_ko not rlike '기사|관리|직원|구_|지하철|버스|대중교통|하이패스|통근|항공|진에어|콜마너|쿠팡 셔틀|현대중공업|핸들모아|셔틀나우|에스원|웰리힐리|금호고속'
                            and description not rlike '기사용|기사님'
                        ) a 
                        left join (
                            select regexp_replace(regexp_replace(af_app_title_eng, '_HTTPS', ''), '_IPv6', '') as af_app_title_eng
                                    ,regexp_replace(regexp_replace(bf_app_title_eng, '_HTTPS', ''), '_IPv6', '') as bf_app_title_eng
                            from ${hivevar:tb_app_title_change}
                            where ym<='202112'   --${hivevar:ym}
                                and bf_app_title_eng <> 'Naver'
                                and bf_app_title_eng <> af_app_title_eng
                            group by af_app_title_eng, bf_app_title_eng
                        ) b 
                        on a.app_title_eng = b.bf_app_title_eng
                      )
                ) t7
              ) t6
            ) t5
          ) t4
        ) t3
      ) t2
      where t2.app_session_duration between 10 and 21600
    ) t1
;

