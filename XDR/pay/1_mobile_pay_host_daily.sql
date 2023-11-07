-- # =====================================================================
-- #  Parameter Setting
-- # =====================================================================

set hive.execution.engine = tez;
set mapreduce.map.memory.mb = 3524;
set mapreduce.map.java.opts = -Xmx2819m;
set mapreduce.reduce.memory.mb = 7048;
set mapreduce.reduce.java.opts = -Xmx5638m;

-- 기준일자
set hivevar:dt = ${hivevar:dt} ; 
set hivevar:dt_from = regexp_replace(date_sub(to_Date(concat(substr(${hivevar:dt}, 1, 4),'-',substr(${hivevar:dt}, 5, 2),'-', substr(${hivevar:dt}, 7, 2))), 30), '-','');

-- 참조 테이블
set hivevar:log_tbl = di_cpm.xdr_filter_total_raw_daily;
set hivevar:host_tbl = di_cpm_etl_dev.mobile_payment_shop_host_daily;

-- 결제 불가 
set hivevar:rmv_app_list1 = 'Google_Map|skt-jive|Toss|Apple_Traffic|Facebook|SamsungIoTCloud|Bixby|sktelecom.com|Gmail|Naver Cafe|SEIO|Instagram|t.co|Newspic|SKT_RCS|FMKorea|Pinterest|Advertising|Akamai|sk.com|live.com|msedge.net|DailyMotion|tand.kr|edgecast|ncloudslb|blismedia.com|ADT CAPS Mobile|ADOT'; 
set hivevar:rmv_app_list2 = 'fb.com|Slack|whoscall|rcsplugin|Namuwiki|ruliweb|me.com|SmartFleet|onedrive|huejura|whatsapp|tistory_com|ksmobile|treffaas|tosspayment|icloud-content|footprint|nhqv|ip6.arpa|0000|chainorder|lawnorder|ShinhanInvest|giphy|ftc.go.kr|adpure|simpli.fi|lencr.org';
set hivevar:rmv_domid_list = 'samsungiotcloud|datarize|scruffapp|oneplatform|sbixby|twitter|userhabit|clova|qgraph|kftc|sweettracker|sktmembership|samsungdive|ttlive|firstservice|0.0.0|bithumb|windows|wikitree|nflxvideo|nflximg|phicdn|nicepay|yahoodns|linecorp|samsungmembers|skt-maap-api|bluen|busanbank|whappsflyer|sentry-cdn|fbsbx|mediatek|everestjs|instabug|bhuroid|vmwservices|maapservice|picknshare|hometax|thecloudberry|16personalities|opensurvey|game-insight|mintegral|cyberlink';
set hivevar:rmv_domain_list = 'channel.io|cre.ma|pki.goog|coov.io|lpay.com|cashwalk.io|cafe24shop|corona-live|cashkeyboard|withinapi|pstatp|pglstatp|samsunghealth|shinhancard|adpdks.kr|ironsrc.mobi|sc.co.kr|braze-image.com|pushimg.com|kebhana.com|yessign.or.kr|dws.co.kr|clarity.ms|addotline.com|adteip|taboola|taboolanews|momento.dev|ealimi.com|d-light.kr|ubcindex.com|cdnga.net|pann.com|appipv4.link|statsig';
set hivevar:rmv_host_list = 'adplatform|safebrowsing|bookmarks|career|support|community|news.naver.com|katalk.kakao.com|paly.melon.com|search.naver.com|news.nil.naver|netmarbleslog|news.like.naver|stat.tiara|cyad1.nate|m.blog.naver|bookmark3.wavve|search.daum|like.naver|m.cafe.daum.net|statclick.nate|beaconqi.qq|news.v.daum|pca.wavve|login|livelog.nexon|v16m.tiktok|p16-sign|v58.tiktok|p16-sg.tiktok|v16.tiktok|image.msscdn|aws.oath.cloud|ktcdn.co.kr|error-tracer|dbill.naver.com|android.apis.google|panorama.map.naver|www.notion.so';

-- 결제 가능
set hivevar:pay_domain = 'kakao|apple|icloud|daum|naver|google|melon|microsoft|tiktok|tmap|netflix|nflix|ahnlab|office|zum|nate|snow.me|band.us|tworld.co.kr|payco.com|postbank|samsungcard|lottecard|bccard|doudou|bebe' ; 
set hivevar:pay_host = 'pay|store|place|buy|shop|order|book|toon|gift|content|money|ticket|checkout|prod|purchase|receipt|storage|bill|wallet|movie|reservation|taxi|bus|merchant|mall';   
-- 결제 수단 제외 
set hivevar:payment_list = 'yeskey|bank|card';
set hivevar:payment_list_host = 'pay.naver.com|pay.naver.net|alipay' ;  


-- # =====================================================================
-- #  테이블 정의
-- # =====================================================================

-- #  결제 가능 앱 (Pay log 직전 log 10개)

create table if not exists di_cpm_etl_dev.mobile_payment_shop_host_daily 
(
    app_title string
    , domain string
    , cat1 string 
    , cat2 string 
    , request_host string 
    , host_freq bigint 
)
partitioned by (dt string)
stored as ORC
;

-- # drop partition 

alter table ${hivevar:host_tbl} drop partition (dt = ${hivevar:dt});

-- # =====================================================================
-- # TABLE 적재 # ---------------------------------------------------------
-- # =====================================================================

with tbl as (
    select *
     , row_number() over(partition by svc_mgmt_num order by request_time asc ) as rn 
    from (
        select *
        from ${hivevar:log_tbl}
        where  dt = ${hivevar:dt}
            and (cat1 <> 'Shopping' or cat1 is null)
            -- and ( cat2 not in ('insurance', 'CryptoCurrency','Stock', 'News', 'AnalyticsPlatform','Bank_Card') 
            --             or request_host rlike ${hivevar:pay_host} )
            -- 결제 불가 
            and app_title not rlike ${hivevar:rmv_app_list1} and app_title not rlike ${hivevar:rmv_app_list2}
            and domid not rlike ${hivevar:rmv_domid_list}
            and domain not rlike ${hivevar:rmv_domain_list}
            and request_host not rlike ${hivevar:rmv_host_list}
            and request_host not like 'ads.%'
            and request_host not like 'ad.%'
            and app_title not like '.%'
            -- 결제 가능 host 별도 처리 
            and request_host not rlike ${hivevar:pay_host}

        union 
        select  * 
        from   ${hivevar:log_tbl}
        where   
            dt = ${hivevar:dt}
            and (cat1 <> 'Shopping' or cat1 is null)
            and domain rlike ${hivevar:pay_domain}
            and request_host rlike ${hivevar:pay_host}
            -- and lower(app_title)
    ) temp 
)

insert overwrite table ${hivevar:host_tbl} partition(dt=${hivevar:dt})

select 
 app_title 
 , domain
 , cat1 
 , cat2 
 , request_host
 , count(distinct svc_mgmt_num, rn) as host_freq  
from (
    select  
     t1.svc_mgmt_num
     , case when t1.app_title = 'Google' and t1.domain not rlike 'google|goo.gl' then t1.domain else t1.app_title end as app_title 
     , t1.domain
     , t1.cat1
     , t1.cat2 
     , t1.request_host
     , t4.rn 
    from  tbl t1 
    left join (
        select 
        t1.* 
        , abs(t2.second_s - t1.second_s) as second_diff
        , t2.second_s as pay_second
        , t2.payment_host 
        from (
            select *
            , 3600*cast(hour as int) + 60*cast(minute as int) + cast(second as int) as second_s
            from tbl
        ) t1 
        join (
            select  *
            -- from di_cpm_dev.online_pay_log_daily
            from di_crowd.pay_log_mobile_daily 
            where exec_dt = ${hivevar:dt}
                and payment_host is not null
        ) t2
        on t1.svc_mgmt_num = t2.svc_mgmt_num
        and t1.request_host = t2.payment_host 
        where abs(t2.second_s - t1.second_s) <= 180
    ) t4
    on t1.svc_mgmt_num = t4.svc_mgmt_num
    where 
    abs(t4.rn - t1.rn) between 0 and 10 
    and t1.request_host <> t4.payment_host 
    and (t1.app_title not rlike ${hivevar:payment_list} or t1.request_host rlike ${hivevar:pay_host} ) 
    and t1.request_host not rlike ${hivevar:payment_list_host}
) t5 
group by  
 app_title 
 , domain
 , cat1 
 , cat2 
 , request_host
; 

-- # =====================================================================
-- #  결제 가능 App/Web 추출 
-- # =====================================================================

-- 기준일자
set hivevar:host_tbl = di_cpm_etl_dev.mobile_payment_shop_host_daily ;
set hivevar:dt = ${hivevar:dt};
set hivevar:dt_from = regexp_replace(date_sub(to_Date(concat(substr(${hivevar:dt}, 1, 4),'-',substr(${hivevar:dt}, 5, 2),'-', substr(${hivevar:dt}, 7, 2))), 30), '-','');

-- # =====================================================================
-- #  테이블 정의
-- # =====================================================================

create table if not exists di_cpm_etl_dev.mobile_payment_shop_daily 
(
    app_title string
    , cat1 string 
    , cat2 string 
    , tot_cnt bigint 
    , host_set string 
)
partitioned by (dt string)
stored as ORC
;

insert overwrite table di_cpm_etl_dev.mobile_payment_shop_daily partition(dt=${hivevar:dt})

select 
 app_title
 , cat1
 , cat2 
 , sum(host_freq) as tot_cnt 
 , concat_ws(',', collect_set(request_host)) as host_set
from ${hivevar:host_tbl}
where dt between ${hivevar:dt_from} and ${hivevar:dt}
group by 
 app_title
 , cat1
 , cat2 
; 
