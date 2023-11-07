-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

-- 기준일자
set hivevar:dt = ${hivevar:dt};
set hivevar:ym = substring(${hivevar:dt}, 1,6) ; 

-- tables
set hivevar:session_tbl = di_cpm.xdr_filter_app_session_daily ;
set hivevar:shop_tbl = di_cpm_etl_dev.mobile_payment_shop_daily;
set hivevar:transport_tbl = di_cpm_etl_dev.mobile_payment_transport_session_daily;
set hivevar:final_tbl = di_cpm_etl_dev.mobile_payment_session_daily; 

-- filtering
set hivevar:rmv_host_list = 'nflxvideo|nflximg|nflxso|netflixdnstest|slackb|slack|netflixsurvey|crashlytics|nid.naver|polling.finance.naver|page.link|soft-berry|tservice.co|adplatform|safebrowsing|bookmarks|career|support|community|io.mi.com|cafe.naver|kiwoom|search|zeropaypoint|app-route|event.kakaopay|business.kakao|wwwwwwww.shop|pay.naver.net|bookmark.naver|ncloudslb|adcenter|vortex.data.micro|news.naver|news.like';
set hivevar:rmv_app_list = 'Shoppinghow|Conects|Taobao|Newspic|LotteON|Naver Blog|Taobao|MiraeAsset|Cashwalk|Facebook|Naver Mail|SmartStoreCenter|BNK|NH|nonghyup|hahabank|BaeminOrder|ntry.com|Wooribank|Starbanking|KakaoMap|ADOT' ; 
set hivevar:pay_host = 'pay|store|place|buy|shop|book|toon|order|gift|content|money|ticket|checkout|prod|purchase|receipt|storage|bill|wallet|movie|reservation|taxi|bus|merchant|subs|ohsara.okcashbag|pay.okcashbag|bzm-capi.kakao';
set hivevar:subs_app = 'kukka|Kukka|ClosetShare|Laundrygo|Lazy Society|PillyCare|Wisely|favv|Snek|Outstanding|amberweather' ;
set hivevar:subs_host = 'wiselyshave|laundrygo|lazysociety|happymoonday|closetshare|kukka|mehisox|pinzle|opengallery|pilly|toun28craft|monthlycosmetics|dolobox|sooldamhwa|purpledog|beanbrothers|flybook|publy|outstanding|beanbrothers|snek|amberweather';
set hivevar:cnt_value = 1000; 

-- # =====================================================================
-- #  테이블 정의
-- # =====================================================================

--  create table if not exists di_cpm_etl_dev.mobile_payment_session_daily
--  (
--   svc_mgmt_num string
--   ,sex_cd string
--   ,cust_age_cd string
--   ,source string
--   ,app_id string
--   ,app_title string
--   ,app_title_eng string
--   ,app_title_ko string
--   ,cat1 string
--   ,cat2 string
--   ,request_host string
--   ,domain string
--   ,subdomain string
--   ,domid string
--   ,request_referer string
--   ,request_referer_domid string
--   ,hour string
--   ,minute string
--   ,second string
--   ,request_time string
--   ,sec int
--   ,app_session_id string
--   ,app_session_duration int
--   ,delta_up_link_data_size double
--   ,delta_dn_link_data_size double
--   ,second_s double
--   ,shop_session_start_time  double
--   ,shop_session_end_time  double
--   ,app_category string 
--  )
--  partitioned by (dt string)
--  stored as ORC
--  ;

-- # =====================================================================
-- #  최종테이블 파티션 적재
-- # =====================================================================


insert overwrite table ${hivevar:final_tbl} partition(dt=${hivevar:dt})

select
   *
   ,from_unixtime(cast(request_time as int), 'HH')*3600 + from_unixtime(cast(request_time as int), 'mm')*60 + from_unixtime(cast(request_time as int), 'ss') as second_s
   ,cast(min(request_time) over(partition by svc_mgmt_num, app_session_id) as double) as shop_session_start_time
   ,cast(max(request_time) over(partition by svc_mgmt_num, app_session_id) as double) as shop_session_end_time
   ,if(cat1 is not null, concat(cat1, '_', cat2), null) as app_category
from (
    select
       t1.*
    from (
      -- Shopping category, 구독 
      select 
        svc_mgmt_num 
        ,sex_cd 
        ,cust_age_cd
        ,source
        ,app_id
        ,app_title
        ,app_title_eng
        ,app_title_ko
        ,cat1
        ,cat2
        ,request_host
        ,domain
        ,subdomain
        ,domid
        ,request_referer
        ,request_referer_domid
        ,hour
        ,minute
        ,second
        ,request_time
        ,sec
        ,app_session_id
        ,app_session_duration
        ,delta_up_link_data_size
        ,delta_dn_link_data_size      
      from  ${hivevar:session_tbl} 
      where 
        dt = ${hivevar:dt} 
        -- 결제 가능 App 
        and (
          cat1 = 'Shopping'    -- shopping 
          or app_title_eng rlike ${hivevar:subs_app} -- 구독 app
          -- 구독 및 기타 web
          or domid rlike ${hivevar:subs_host}
          or domid rlike 'bustago|nike|skinnylab|mall|boribori|taobao'  -- 기타 포함 app/web 
          or app_title_eng rlike 'Wibee|Aladin' -- 기타 포함 app
        )
        and request_host not rlike ${hivevar:rmv_host_list}
        and app_title not rlike ${hivevar:rmv_app_list}
        and substring(svc_mgmt_num,1,2) = 's:'

      -- shopping 외 기준 만족하는 host/app 
      union 
      select 
        svc_mgmt_num 
        ,sex_cd 
        ,cust_age_cd
        ,source
        ,app_id
        ,app_title
        ,app_title_eng
        ,app_title_ko
        ,cat1
        ,cat2
        ,request_host
        ,domain
        ,subdomain
        ,domid
        ,request_referer
        ,request_referer_domid
        ,hour
        ,minute
        ,second
        ,request_time
        ,sec
        ,app_session_id
        ,app_session_duration
        ,delta_up_link_data_size
        ,delta_dn_link_data_size      
      from  ${hivevar:session_tbl} 
      where 
        dt = ${hivevar:dt} 
        and cat1 <> 'Shopping'
        and cat2 not rlike 'Transport|sharing|Ride_request'
        and app_title_eng not rlike ${hivevar:subs_app} -- 구독 app
        and domid not rlike ${hivevar:subs_host}  -- 구독 web 
        and app_title_eng not rlike 'Wibee|Aladin' -- 기타 포함 app
        and request_host not rlike ${hivevar:rmv_host_list}
        and app_title not rlike ${hivevar:rmv_app_list}
        and app_title in (
          select distinct app_title 
          -- from ${hivevar:shop_tbl}
          -- where dt = ${hivevar:dt} 
          from smkim_payment_meta_app_monthly
                -- and (tot_cnt >= ${hivevar:cnt_value} or cat2 = 'Food' or host_set rlike ${hivevar:pay_host})
        )      
        and substring(svc_mgmt_num,1,2) = 's:'

      -- transport session 
      union 
      select 
        svc_mgmt_num 
        ,sex_cd 
        ,cust_age_cd
        ,source
        ,app_id
        ,app_title
        ,app_title_eng
        ,app_title_ko
        ,cat1
        ,cat2
        ,request_host
        ,domain
        ,subdomain
        ,domid
        ,request_referer
        ,request_referer_domid
        ,hour
        ,minute
        ,second
        ,request_time
        ,sec
        ,app_session_id
        ,app_session_duration
        ,delta_up_link_data_size
        ,delta_dn_link_data_size
      from ${hivevar:transport_tbl}
      where dt=${hivevar:dt} 
        and substring(svc_mgmt_num,1,2) = 's:'
    ) t1 
    left join (
      select  distinct domain
      from    default.jym_nielsen_category 
      where   ym = '202106'
              and (
                (cat1 in ('커뮤니티') and cat2 not rlike 'SNS|블로그')
                or cat2 rlike '증권사|보험|경제지|홈페이지제작|소프트웨어|기타 인터넷|고등학교|신용카드|대사관|군사|기타금융'
              )
              and domid not rlike 'onestore|cafe24|auction|tradingview|office'
    ) t2
    on t1.domain = t2.domain
    where t2.domain is null
) temp 

;