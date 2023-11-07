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
set hivevar:host_tbl = di_cpm_etl_dev.mobile_payment_shop_host_daily ;

-- 결제 불가 
set hivevar:rmv_app_list1 = 'Google_Map|skt-jive|Toss|Apple_Traffic|Facebook|SamsungIoTCloud|Bixby|sktelecom.com|Gmail|Naver Cafe|SEIO|Instagram|t.co|Newspic|SKT_RCS|FMKorea|Pinterest|Advertising|Akamai|sk.com|live.com|msedge.net|DailyMotion|tand.kr|edgecast|ncloudslb|blismedia.com'; 
set hivevar:rmv_app_list2 = 'fb.com|Slack|whoscall|rcsplugin|Namuwiki|ruliweb|me.com|SmartFleet|onedrive|huejura|whatsapp|tistory_com|ksmobile|treffaas|tosspayment|icloud-content|footprint|nhqv|ip6.arpa|0000|chainorder|lawnorder|ShinhanInvest|giphy|ftc.go.kr|adpure|simpli.fi|lencr.org';
set hivevar:rmv_domid_list = 'samsungiotcloud|datarize|scruffapp|oneplatform|sbixby|twitter|userhabit|clova|qgraph|kftc|sweettracker|sktmembership|samsungdive|ttlive|firstservice|0.0.0|bithumb|windows|wikitree|nflxvideo|nflximg|phicdn|nicepay|yahoodns|linecorp|samsungmembers|skt-maap-api|bluen|busanbank|whappsflyer|sentry-cdn|fbsbx|mediatek|everestjs|instabug|bhuroid|vmwservices|maapservice|picknshare|hometax|thecloudberry';
set hivevar:rmv_domain_list = 'channel.io|cre.ma|pki.goog|coov.io|lpay.com|cashwalk.io|cafe24shop|corona-live|cashkeyboard|withinapi|pstatp|pglstatp|samsunghealth|shinhancard|adpdks.kr|ironsrc.mobi|sc.co.kr|braze-image.com|pushimg.com|kebhana.com|yessign.or.kr|dws.co.kr|clarity.ms|addotline.com|adteip|taboola|taboolanews|momento.dev|ealimi.com|d-light.kr|ubcindex.com|cdnga.net|pann.com|appipv4.link';
set hivevar:rmv_host_list = 'adplatform|safebrowsing|bookmarks|career|support|community|news.naver.com|katalk.kakao.com|paly.melon.com|search.naver.com|news.nil.naver|netmarbleslog|news.like.naver|stat.tiara|cyad1.nate|m.blog.naver|bookmark3.wavve|search.daum|like.naver|m.cafe.daum.net|statclick.nate|beaconqi.qq|news.v.daum|pca.wavve|login|livelog.nexon|v16m.tiktok|p16-sign|v58.tiktok|p16-sg.tiktok|v16.tiktok|image.msscdn|aws.oath.cloud|ktcdn.co.kr|error-tracer|dbill.naver.com';

-- 결제 가능
set hivevar:pay_domain = 'kakao|apple|icloud|daum|naver|google|melon|microsoft|tiktok|tmap|netflix|nflix|ahnlab|office|zum|nate|snow.me|band.us|tworld.co.kr|payco.com|postbank|samsungcard|lottecard|bccard' ; 
set hivevar:pay_host = 'pay|store|place|buy|shop|order|book|toon|gift|content|money|ticket|checkout|prod|purchase|receipt|storage|bill|wallet|movie|reservation|taxi|bus|merchant|mall';
   
-- 결제 수단 제외 
set hivevar:payment_list = 'yeskey|bank|card';
set hivevar:payment_list_host = 'pay.naver.com|pay.naver.net|alipay' ;  


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

-- create table if not exists di_cpm_etl_dev.mobile_payment_shop_daily 
-- (
--     app_title string
--     , cat1 string 
--     , cat2 string 
--     , tot_cnt bigint 
--     , host_set string 
-- )
-- partitioned by (dt string)
-- stored as ORC
-- ;

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
