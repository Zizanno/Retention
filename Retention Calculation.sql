select cohort_users
,case when date_diff(date(CURRENT_DATETIME()),D0,day)<=1 then null else day_1 end as day_1
,case when date_diff(date(CURRENT_DATETIME()),D0,day)<=2 then null else day_2 end as day_2
,case when date_diff(date(CURRENT_DATETIME()),D0,day)<=3 then null else day_3 end as day_3
,case when date_diff(date(CURRENT_DATETIME()),D0,day)<=7 then null else day_7 end as day_7
, PLATFORM
, D0
, client_version
, build_number
, country
, quality_setting
, device_model 
, split(device_model, ' ')[offset (0)] device_company
, operating_system 
, operating_system_detail
, case when media_source is null then 'no_advertising_id' else media_source end as media_source
, af_adset 
, af_ad
, fb_campaign_name
, fb_adgroup_name
, campaign
, case when media_source='googleadwords_int' and campaign='RND - MB - UK - 150.3.0 - IAA/Tutorial' then 'Build Quality Campaigns'
when media_source='Facebook Ads' and campaign='RnD_MB_UK_BuildQuality_Android_App_Install' then 'Build Quality Campaigns'
when media_source='unityads_int' and campaign='RnD_MB_Unity_BuildQuality_wifi' then 'Build Quality Campaigns'
else 'Test Campaigns' end as campaign_source
, seconds as total_missions_completed
, ddiff_max_date
, last_date
, case when ddiff_max_date>0 then 1 else 0 end as D1_Rolling
from
(
  select
    sum(case when ddiff = 0 then 1 else 0 end) as cohort_users
    , sum(case when ddiff = 1 then 1 else 0 end) as day_1
    , sum(case when ddiff = 2 then 1 else 0 end) as day_2
    , sum(case when ddiff = 3 then 1 else 0 end) as day_3
    , sum(case when ddiff = 7 then 1 else 0 end) as day_7
    , PLATFORM
    , D0
    , client_version
    , country
    , split(quality_setting,'(')[offset(0)] as quality_setting
    , build_number
    , device_model
    , split(split(operating_system, ' ' )[offset(2)],'.')[offset(0)] as operating_system
    , split(operating_system, ' ' )[offset(2)] as operating_system_detail
    , media_source
    , af_adset 
    , af_ad
    , fb_campaign_name
    , campaign
    , fb_adgroup_name
    , seconds
    , ddiff_max_date
    , last_date
  from(  
      select
      D0
      , a.user_id
      , b.dt
      , date_diff(b.dt,a.D0,day) as ddiff
      , country
      , client_version
      , platform
      , build_number
      , device_model 
      , operating_system 
      , split(quality_setting,'(')[offset(0)] as quality_setting
      , media_source
      , af_adset 
      , af_ad
      , fb_campaign_name
      , campaign
      , fb_adgroup_name
      , seconds
      , last_date
      , date_diff(a.last_date,a.D0,day) as ddiff_max_date
     from
     (   
        select min(D0) as D0
        , user_id
        , max(D0) as last_date
        from(
        select cast(split(split(message, ",")[offset(2)], ")")[offset(0)] as int64) as user_id 
        , date(event_timestamp) as D0
        from dw_mtlb_live.session_start   
        )a
        group by user_id
    )a    
     JOIN
    (
        select dt
        , user_id
        from(
        select cast(split(split(message, ",")[offset(2)], ")")[offset(0)] as int64) as user_id 
        , date(event_timestamp) as dt
        from dw_mtlb_live.session_start   
        )a
        group by dt, user_id
    )b
    on a.user_id=b.user_id 
    left join
    (
        select *
      from (
         SELECT min(((date(event_timestamp)))) AS D01
        , user_id
        , country
        , client_version
        , platform
        , quality_setting
        , build_number
        , device_model 
        , operating_system
        , rank() over ( partition by  user_id order by min(((date(event_timestamp)))),country,client_version,platform,quality_setting,build_number,device_model,operating_system ) as ranks
        from dw_mtlb_live.client_device 
        group by  user_id , country  , client_version   , platform ,quality_setting,build_number,device_model,operating_system
        order by user_id
          )g
        where ranks=1    
    )c
    on a.user_id=c.user_id
      left join
    (
      select *
      from
      (
      SELECT   rank() over (partition by advertising_id  order by  install_time,customer_user_id , media_source,carrier,city,fb_adset_name,app_version,af_adset ,af_ad, fb_campaign_name,campaign,fb_adgroup_name
 ) as rank_
      ,media_source
      ,carrier
      ,city
      ,fb_adset_name
      ,app_version
      ,customer_user_id
      ,advertising_id
      , install_time 
      , af_adset 
      , af_ad
      , fb_campaign_name
      , campaign
      , fb_adgroup_name
      FROM `dw_appsflyer.af_raw_events`  
      where app_name='Metalborne: Mech combat of the future'
      and customer_user_id is not null
      and event_type ='install'
      order by rank_ desc
      )a
      where rank_=1  
        )d
      on a.user_id=cast(d.customer_user_id as INT64)
      
          left join
    (
    select count(distinct mission_id) as seconds
    , user_id
    from dw_mtlb_live.mission_completed
    group by user_id
    
    
   )e
   on a.user_id=e.user_id
   
  )a
  where client_version>='0.150.3.0'
  group by PLATFORM
    , D0
    , client_version
    , country
    , quality_setting
    , build_number
    , device_model 
    , operating_system  
    , operating_system_detail
    , media_source
    , af_adset 
    , af_ad
    , fb_campaign_name
    , campaign
    , fb_adgroup_name
    , seconds
    , ddiff_max_date
    , last_date
 )x
order by D0