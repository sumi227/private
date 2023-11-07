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
-- tables
set hivevar:session_tbl = di_cpm_etl_dev.mobile_payment_session_daily;
set hivevar:final_tbl = di_cpm.mobile_payment_mapping_daily; 
-- filtering 
set hivevar:pay_host = 'pay|store|place|buy|shop|book|toon|order|gift|content|money|ticket|checkout|prod|purchase|receipt|storage|bill|wallet|movie|reservation|taxi|bus|merchant|subs|ohsara.okcashbag|nearby|town';
set hivevar:strming_app_list = 'WAVVE|Netflix|Watcha|Tving|Disney|Apple TV|Melon|FLO|Geniemusic|Bugs|Soribada|Tiktok|Podbbang|SBS|MBC|Discord|OKCashBag|Syrup|Apple_Traffic|Cashnote|SKT_T-Map|Galaxiapay|Zoomus|Microsoft|Between|PPOMPPU|Bungae|Daangn|Syrup|Lpoint' ; 
set hivevar:strming_domid_list = 'melon|music-flo|bugs|genie|musicmates|wavve|soribada|sndcdn|mureka|netflix|watcha|tving|cjenm|youtube|afreecatv|discord|twitch|pooq|applemusic|apple|itunes|microsoft|starvoice|ppomppu|bunjang|karrotmarket|daangn|syrup|lpoint'; 
set hivevar:rmv_host_list = 'vortex.data.micro|kbstar|wooribank|mktp.tmoney|agd_policy|agd-policy|app.tworld|setup.icloud|dmaps.daum|map.kakao|chat.google|www.google.co|cdnm.tworld|ticket-loco.kakao|shopdp-api|business.naver|scrapbook.naver|jne.kr|tumblr|kbanknow|kakaobank|cu.co.kr|sktapollo.com';

-- # =====================================================================
-- #  테이블 정의
-- # =====================================================================

-- create table if not exists di_cpm.mobile_payment_mapping_daily (
--    svc_mgmt_num string
--    ,sex_cd string
--    ,cust_age_cd string
--    ,app_title_ko  string
--    ,app_title_eng string
--    ,app_category string
--    ,shop_cd string
--    ,shop_category string 
--    ,shop_session_id string
--    ,shop_session_duration int
--    ,shop_session_start_time float
--    ,shop_session_end_time float
--    ,shop_session_start_second_s int
--    ,shop_session_end_second_s int
--    ,pay_second_s  int
--    ,payment_mms_name  string
--    ,payment_mms_num string
--    ,payment_host  string
--    ,payment_domid  string
--    ,request_host  string
--    ,request_host_domid string
--    ,request_referer  string
--    ,request_referer_domid string
--    ,first_host string
--    ,first_referer string
--    ,first_referer_domid string
--    ,host_set string
--  )
--  partitioned by (dt string)
--  stored as ORC
--  ;


-- # =====================================================================
-- #  최종 테이블 적재
-- # =====================================================================

insert overwrite table ${hivevar:final_tbl} partition(dt = ${hivevar:dt})

select 
 svc_mgmt_num
 , sex_cd
 , cust_age_cd
 , case when request_host_domid = 'kurly.services' then '마켓컬리' else app_title_ko end as app_title_ko
 , case when request_host_domid = 'kurly.services' then 'MarketKurly' else app_title_eng end as app_title_eng
 , case when request_host_domid = 'kurly.services' then 'Shopping_Fresh_delivery' else app_category end as app_category
 , case when request_host_domid = 'kurly.services' then 'MarketKurly' else shop_cd end as shop_cd
 -- Category 
 , case 
      -- # food
      when app_category rlike 'Food' or request_host_domid in ('pji','foodjang','handmadepizza','pizzahut','foodspring','tpirates','thebenefood','foodingfactory','eatsslim','samjinfood','foodyap','leanfood','woowa','woowahan') 
           or request_host rlike 'dm.pulmuone|woowahan.com|mealticket' 
           then 'food'  
      -- # 교통/이동수단 
      when app_category rlike 'Location_sharing|Location_Transport|Location_Ride_request' or request_host_domid in ('bustago','socarcorp','alpaca','happycarservice')
           or shop_cd in ('Rentnow','Rentking','Dolarupang','JejupassRentcar','KakaoMobility','SKT_T-Map','Lime') 
           then 'transport'
      -- # 여행/호텔 
      when app_category rlike 'Travel|Hotel' or request_host_domid in (
             'yanolja','saletonight','sonohotelsresorts','daemyungresort','airbnb','expedia','hotelpass','hotelscombined','hotels','hoteltime','hotelnow','hotelnjoy','hotelgoto','tripadvisor','myrealtrip','jejumobile','bearcreek','foresttrip','twayair','webtour','flyasiana',
             'jinair','koreanair','flyairseoul','jejudo','hi-airlines','aircanada')
           or request_host rlike 'eticket.seogwipo'
           then 'travel'
      -- # 영화/공연
      when trim(shop_cd) in ('CGV','lottecinema','maxmovie_com','megabox_co_kr','mcinepox','Yes24Movie', 'daehancinema','InterparkTicket','MelonTicket','ticketlink','timeticket','ticketbay','Ticketbay')
           or request_host rlike 'showmovie.mobile.kt|acc.go.kr|maketicket|bscc.or.kr|enticket|cwcf'
           or (request_host rlike 'movie|ticket' and request_host_domid rlike 'tmembership|naver|daum|interpark|melon|interpark|ktwiz|yes24|wemakeprice')
        then 'movie/perform'
      -- # 구독 상품 
      when request_host_domid rlike 'wiselyshave|laundrygo|lazysociety|happymoonday|closetshare|kukka|mehisox|pinzle|opengallery|pilly|toun28craft|monthlycosmetics|dolobox|sooldamhwa|purpledog|beanbrothers|flybook'
           or request_host_domid rlike 'delight.weeat' then 'product_subs'
      -- # Book/Cartoons 
      when app_category rlike 'Cartoon' 
           or (trim(shop_cd) in ('Aladin', 'KyoboBook', 'Yes24.com','Munpia','ridibooks_com','joara_com','Aladin Bookstore','Naver Series','Bookcube') and request_host not like 'ticket')
           or request_host_domid rlike 'webtoon|millie|bandinlunis|bookcrew|ridibooks|bookjournalism|welaaa|joara|laftel|mrblue|storytel|onestorebooks|books.onestore|onestore|toptoon|comico|comica|lezhin|ypbooks|moonpia|ridicdn.net|11toon2|bookcosmos|bookcube|bookoa|kyobobook|lifebook'
           or request_host rlike 'webtoons.naver|book.naver|book.interpark'
         then 'book/cartoon'
      -- # Communication
      when app_category rlike 'Messenger|Dating' or trim(shop_cd) in ('Discord')
           or request_host rlike 'strangerchat|goodnight|ggosso|micoworld|badoo|badoocdn|thebermuda|eundabang|jaumo|couplemaker|tantanapp|tancdn|tinder|diamatch|strangerchat|appintalk'
         then 'communication'
      -- # MUSIC
      when app_category rlike 'Music'
           or request_host rlike 'musicmates|music-flo|melon|bugs|vibe.naver|vibeapp|soribada|geniemusic|genie.co.kr|music.kakao.com'
         then 'music'
      -- # Video
      when app_category rlike 'Video_Broadcasting|sVOD'
           or trim(shop_cd) rlike 'Podbbang|Twitch|WAVVE|BigoLive'
           or request_host rlike 'watcha|netflix|nflxvideo|nflximg|bflix|youtube|tving|cjenm.com|gomtv|afreecatv.com|ttvnw|hakuna|sbs.co.kr|spooncast'
         then 'video/streaming'
      -- # News / Information
      when request_host_domid rlike 'folin|outstanding|publy|snek' then 'news/info'
      -- # Entertainment
      when request_host_domid rlike 'yantech|izone-mail|smule|smle|lysn|magicsing|mubeat|tvbaduk|starplay|everysing|weverse|justdancenow|jdnowweb|myloveidol|myloveactor|getkeepsafe|sbsgolf|filekuki|ntry|wedisk|megafile|livescore|spotvnews|filenori|applefile'
           or request_host rlike 'golf.sbs|named.com|spotv.net|filecity.co.kr|bigfile.co.kr|bigfile.pe.kr|sharebox.co.kr|kdisk.co.kr|pdpop.com'
           or trim(shop_cd) in ('my_K','Meitu','Inshot', 'Kakao TV','KakaoPage') or app_category rlike 'Entertainment'
         then 'entertainment'
      -- # Game
      when app_category rlike 'Game' or lower(trim(shop_cd)) rlike 'hilclimb|raid'
           or request_host_domid rlike 'devsgb|pokemon-home|armyneedyou|blizzard|cookingadventure|tgame365|perplelab|snowpipe|flerogame|playdemic|netmarble|hangame'
           or request_host rlike 'artofwar|battle.net|rivalstars|roblox|lastshelter|btsworld|stzapp|anipang|supersonic|nc.com|lilith'
           or (request_host_domid = 'kakao' and request_host rlike 'game|play')
         then 'game'
      -- # Education
      when app_category rlike 'Education'
           or request_host_domid rlike 'duolingo|littlefox|mondlylanguages|ptvmob|studyhelper|iwing|alphalaw|airklass|wafour|tomtimstudio|bluepin|kakaokids|tandem|todomath|todoschool|pinkfong|hellochinese|hellotalk8|kebikids|gnbenglish|safetyedu'
           or request_host rlike 'tutoring.co.kr|tutoring2.remotemonster.com|uphone.co.kr|kifin.or|edu.ingang.go.kr|nexusbook|nebooks|bookmouse|bookisland'
         then 'education'
      -- # Finance
      when request_host_domid rlike 'investing|forexpros|dunamu|wowtv|hankyung|credit|allcredit|tradingview' then 'credit/invest'
      -- # Life
      when request_host rlike 'mabopractice|neuronation|herokudns|noom|relive|mydano|life360|blimp|soundgym|daybabyday|strava|weatherlive|isharing|alfred|barunsoncard|mticket.lotteworld'
           or request_host rlike 'yazio|openrider|windy|1km|jobplanet|wachanga|jeomsin|calm|kokkiri|fatsecret|pghome|hreum|lottorich|class101|golfzon|psynet|gg.go.kr|booking.naver'
           or (request_host_domid rlike 'kakao|naver' and request_host rlike 'booking') or shop_cd rlike 'Naver Map|NaverSmartplace|Everytime'
           or app_category rlike 'Lifestyle'
         then 'lifestyle'
      -- # Utility
      when request_host rlike 'evernote|vllo|dropbox|vimosoft|dwgfastview|itranslateapp|itranslate|nordvpn|nord-app|zwyr157wwiu6eior|gopro|mega|safe4kid|autocad|canva.com|casemaster|kinemaster|faceapp|polarisoffice|fineapptech|substrate.office|fontawesome|cloudstorage|storage.naver|storage.cloud'
           or lower(trim(shop_cd)) rlike 'life360|nordvpn|vivavideo|dropbox|naver mybox'
         then 'utility'
      -- # 기타
      when request_host rlike 'sc.or.kr|hopeon.or|childfund.or|goodneighbors|unicef.or|beautifulfund|joyagdol|miral.or|worldvision.or|purme.or|unhcr.or|jts.or.kr|mrmweb.hsit.co.kr|compassion.or|kfhi.or|habitat.or|busrugy.or|dail.org|plankorea.or|kclf.org|eastern.or|salvationarmy|eugenebell'
         then 'donation'
      when request_host rlike 'sc.go.kr|iros.go.kr|giro.or.kr|wetax.go.kr|nhis.or.kr|tax.seoul.go|carhistory.or|lx.or.kr|apply.lh.or.kr|scourt.go.kr|seoulgas|apti.co.kr'
         then 'tax/bill/doc'
      when request_host rlike 'safedriving.or|q-net.or|kotsa.or|opic.or.kr|kpc.or.kr|kofia.or.kr|pss.go.kr|kcg.go.kr|in.or.kr|kuksiwon.or.kr|klt.or.kr|nfoodedu.or.kr|sac.or.kr|kisq.or.kr|kna.or.kr'
         then 'certi/apply/rgtr'
      when request_host rlike 'kosaf.go|.hf.go.kr|kinfa.or.kr|kfsi.or.kr' then 'loan'
      when request_host rlike 'game' then 'game'
      -- # Shopping
      when shop_cd <> 'Naver' and (
           app_category rlike 'Shopping' or shopping_yn = 'Y' and shop_cd not rlike 'Pay'
           or request_host_domid in ('gsretail','ezwel','kurly.services','amway') 
           or (request_host_domid = 'kakao' and request_host rlike 'pay|gift|store|money|shopping|order|buy|billgate')
           or trim(shop_cd) in ('Naver_SmartStore','musinsa_com','Taobao','AmorePacificMall','LotteHomeshopping','Ably','Hmall','g9','SSF Shop','Idus','Homeplus','MyHomeplus','NSmall_com','Pocket CU',
             'Halfclub','Alibaba','Elandmall','HomeshoppingMoa','GSFresh','ebay korea','SSG.COM','cjmall','Kakao_Store','gsshop','enuri_com','EZwel','HouseOfToday','Ezwel','Memebox'))
         then 'shoppingmall'
      else 'etc'
   end as shop_category 
 , app_session_id as shop_session_id
 , app_session_duration as shop_session_duration
 , cast(shop_session_start_time as double) as shop_session_start_time
 , cast(shop_session_end_time as double) as shop_session_end_time
 , shop_session_start_second_s
 , shop_session_end_second_s
 , pay_second_s
 , payment_mms_name
 , payment_mms_num
 , payment_host
 , payment_domid
 , request_host
 , request_host_domid
 , request_referer
 , request_referer_domid
 , first_host
 , first_referer
 , first_referer_domid
 , host_set
from (
  select
    t3.svc_mgmt_num
    ,t3.sex_cd
    ,t3.cust_age_cd
    ,t3.shop_cd
    ,t3.app_title_ko
    ,t3.app_title_eng
    ,t3.app_category
    ,t3.app_session_id
    ,t3.app_session_duration
    ,t4.shop_session_start_time
    ,t4.shop_session_end_time
    ,t4.shop_session_start_second_s
    ,t4.shop_session_end_second_s
    ,t3.pay_second_s
    ,t3.payment_mms_name
    ,t3.payment_mms_num
    ,t3.payment_host
    ,t3.payment_domid
    ,t3.request_host
    ,t3.request_host_domid
    ,t3.request_referer
    ,t3.request_referer_domid
    ,t4.first_host
    ,t4.first_referer
    ,t4.first_referer_domid
    ,t4.host_set
    -- ,row_number() over(partition by t3.svc_mgmt_num, t3.shop_cd order by t3.pay_second_s) as rn
    ,t3.rn
    ,t3.shopping_yn 
  from (
    -- 세션화 테이블에 결제 추정 붙임
    select
      svc_mgmt_num
      ,sex_cd
      ,cust_age_cd
      ,case when app_title rlike 'Google|AWS Region' and domid not rlike 'google' then domain 
            when domid in ('daehancinema') then domain 
            when domid = 'kakao' and request_host rlike 'tv' then 'Kakao TV'
            when domid = 'kakao' and request_host rlike 'page' then 'KakaoPage'
            else app_title end as shop_cd 
      ,app_title_eng 
      ,app_title_ko 
      ,app_category
      ,app_session_id
      ,app_session_duration
      ,pay_second_s
      ,payment_mms_name
      ,payment_mms_num
      ,payment_host
      ,payment_domid
      ,request_host
      ,domid as request_host_domid
      ,request_referer
      ,request_referer_domid
      ,row_number() over(partition by svc_mgmt_num, pay_second_s order by match_yn asc, priority asc, pay_yn asc, pay_second_diff asc ) as rn  -- 우선순위:shoppingmall > general > music/video
      ,shopping_yn
    from (
      -- mapping 1: general
      select
        t31.*
        , case when t31.app_title rlike 'Naver_SmartStore' then 1 
               when t31.app_category rlike 'Food' then 2 
               when t31.request_host rlike ${hivevar:pay_host} and t31.request_host not rlike 'bill.nhn|bill.naver|billg.naver|billx.naver|checkout.naver' and t31.app_category not rlike 'Payment|Bank|Card' then 3
               when t31.app_title rlike 'Naver Shopping' or t31.app_category rlike 'PCS' then 4
               when (t32.domid is not null or t31.app_category rlike 'Shopping') and t31.app_category not rlike 'Discounts|Used|PCS|Payment' and app_title_ko not rlike '홈쇼핑모아|에누리' then 1 
               when (t32.domid is not null or t31.app_category rlike 'Shopping') then 5
               when t31.app_category rlike 'Navigation|Messenger|Payment|Portal|Bank|Card' or app_title rlike ${hivevar:strming_app_list} or t31.request_host not rlike 'bill.nhn' then 9 
               else 8 end as priority
        , case when t31.request_host rlike ${hivevar:pay_host} and t31.request_host not rlike 'bill.nhn|bill.naver|billg.naver|billx.naver|checkout.naver' then 1 else 2 end as pay_yn 
        , case when t32.domid is not null or t31.app_category rlike 'Shopping' then 'Y' else 'N' end as shopping_yn
      from (
        select
          t11.*
            ,t21.second_s as pay_second_s
            ,t21.second_s - t11.second_s as pay_second_diff
            ,case 
              when t21.payment_domid rlike 'ebay|smilepay' and t11.app_title rlike 'gmarket|auction_co_kr' then 1 
              when t21.payment_domid = 'sk-pay' and lower(t11.app_title) rlike '11st|sk_stoa|T-Map|UT' then 1 
              when t21.payment_domid = 'ssg' and lower(t11.app_title) rlike 'Emart|SSG mall|Shinsegae' then 1 
              when t21.payment_domid = 'naver' and t11.app_title rlike 'Naver' and t11.app_title rlike 'Pay' and t11.app_category not rlike 'Payment|Portal' then 1 
              when t11.app_category not rlike 'Bank|Card|Payment' and t21.payment_domid <> 'ebay' 
                   and (t11.domid = t21.payment_domid
                        or lower(regexp_replace(regexp_replace(t11.app_title, ' ',''), '_com','')) = t21.payment_domid
                        or default.getdomain(t11.app_title) = t21.payment_domid)
                  then 1
              when t21.payment_domid = 'ebay' 
                   and (t11.domid = t21.payment_domid
                        or lower(regexp_replace(regexp_replace(t11.app_title, ' ',''), '_com','')) = t21.payment_domid
                        or default.getdomain(t11.app_title) = t21.payment_domid)
                  then 2 
              else 9 end as match_yn
            ,t21.payment_mms_name
            ,t21.payment_mms_num
            ,t21.payment_host
            ,t21.payment_domid
          from
          ( -- t1: 2번 테이블
            select *
            from ${hivevar:session_tbl}
            -- 특정 영역 제외
            where 
              dt = ${hivevar:dt}
              and ((
                app_title not rlike ${hivevar:strming_app_list}
                and domid not rlike ${hivevar:strming_domid_list}
                and app_category not rlike 'Video|sVOD|Movie|Music|Radio|Portal|SNS|Messenger'
                and request_host not rlike 'google|facebook'
                and request_host not rlike ${hivevar:rmv_host_list}
              ) 
              or ( 
                (app_title rlike ${hivevar:strming_app_list}  -- Streaming app 
                or domid rlike ${hivevar:strming_domid_list}   -- Streaming app 
                or app_category rlike 'Video|sVOD|Movie|Music|Radio|Portal|SNS|Messenger'  -- Streaming / Messenger Category 
                or request_host rlike 'google' -- portal
                ) and request_host rlike ${hivevar:pay_host}
              ))
          ) t11
          join
          ( -- t2: pay log
            select
              *
            from di_cpm_dev.online_pay_log_daily
            where dt = ${hivevar:dt}
              and payment_host not rlike 'coupang'
              -- and payment_host not rlike 'mpay.samsung.com'
          ) t21
          on t11.svc_mgmt_num = t21.svc_mgmt_num
          where
            t21.second_s
            between
              from_unixtime(cast(t11.shop_session_start_time as int), 'HH')*3600
              + from_unixtime(cast(t11.shop_session_start_time as int), 'mm')*60
              + from_unixtime(cast(t11.shop_session_start_time as int), 'ss')
            and
              from_unixtime(cast(t11.shop_session_end_time as int), 'HH')*3600
              + from_unixtime(cast(t11.shop_session_end_time as int), 'mm')*60
              + from_unixtime(cast(t11.shop_session_end_time as int), 'ss') + 5
            and t21.second_s - t11.second_s > 0
      ) t31
      left join (
        select distinct domid
        from di_cpm_etl_dev.nielsen_shoppingmall_list
      ) t32
      on t31.domid = t32.domid

      union all
      -- mapping2: Coupang, 로켓페이
      select
      t32.*
      , case when app_title rlike 'Eats' then 1 else 2 end as priority
      , 1 as pay_yn 
      , 'N' as shopping_yn
      from (
        select
          t12.*
            ,t22.second_s as pay_second_s
            ,t22.second_s - t12.second_s as pay_second_diff
            ,case when t12.domid = t22.payment_domid
                      or lower(regexp_replace(regexp_replace(t12.app_title, ' ',''), '_com','')) = t22.payment_domid
                      or default.getdomain(t12.app_title) = t22.payment_domid
                  then 1 else 0 end as match_yn
            ,t22.payment_mms_name
            ,t22.payment_mms_num
            ,t22.payment_host
            ,t22.payment_domid
          from
          ( -- t1: 2번 테이블
            select *
            from ${hivevar:session_tbl}
            where dt = ${hivevar:dt} and request_host rlike 'coupang'
          ) t12
          join
          ( -- t2: pay log
            select
              *
            from di_cpm_dev.online_pay_log_daily
            where dt = ${hivevar:dt}
              and payment_host rlike 'coupang'
          ) t22
          on t12.svc_mgmt_num = t22.svc_mgmt_num
          where
            t22.second_s
            between
              from_unixtime(cast(t12.shop_session_start_time as int), 'HH')*3600
              + from_unixtime(cast(t12.shop_session_start_time as int), 'mm')*60
              + from_unixtime(cast(t12.shop_session_start_time as int), 'ss')
            and
              from_unixtime(cast(t12.shop_session_end_time as int), 'HH')*3600
              + from_unixtime(cast(t12.shop_session_end_time as int), 'mm')*60
              + from_unixtime(cast(t12.shop_session_end_time as int), 'ss')
            and t22.second_s - t12.second_s > 0
      ) t32

    ) temp
    -- where rn = 1  -- 세션 시작 마지막 시간 내에서 시간차이가 가장 작은 결제 이력을 해당 세션의 소비로 판단
    distribute by svc_mgmt_num
    sort by app_session_id, pay_second_s

  ) t3
  join
  ( -- t1: 쇼핑 세션 단위로 요약
    select
      svc_mgmt_num
      ,sex_cd
      ,cust_age_cd
      ,shop_cd
      ,app_session_id
      ,min(request_time) as shop_session_start_time
      ,from_unixtime(cast(min(request_time) as int), 'HH')*3600
          + from_unixtime(cast(min(request_time) as int), 'mm')*60
          + from_unixtime(cast(min(request_time) as int), 'ss') as shop_session_start_second_s
      ,max(request_time) as shop_session_end_time
      ,from_unixtime(cast(max(request_time) as int), 'HH')*3600
          + from_unixtime(cast(max(request_time) as int), 'mm')*60
          + from_unixtime(cast(max(request_time) as int), 'ss') as shop_session_end_second_s
      ,collect_list(if(rn=1, request_host, null))[0] as first_host
      ,collect_list(if(rn=1, request_referer, null))[0] as first_referer
      ,default.getdomain(collect_list(if(rn=1, request_referer, null))[0]) as first_referer_domid
      ,concat_ws(',', collect_set(request_host)) as host_set
      from (
        select
          *
          , case when app_title rlike 'Google|AWS Region' and domid not rlike 'google' then domain else app_title end as shop_cd 
          , row_number() over(partition by svc_mgmt_num, app_session_id order by request_time) as rn
        from ${hivevar:session_tbl}
        where dt = ${hivevar:dt}
      ) temp
      group by
        svc_mgmt_num
        ,sex_cd
        ,cust_age_cd
        ,shop_cd
        ,app_session_id
  ) t4
  on t3.svc_mgmt_num = t4.svc_mgmt_num
    and t3.app_session_id = t4.app_session_id
  where t3.rn =1
) t5 
;
