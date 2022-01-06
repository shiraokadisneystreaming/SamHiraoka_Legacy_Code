--For bundle dashboard
set start_Date = '2021-11-01';
set end_Date = '2021-12-12';
-------------
CREATE OR REPLACE TABLE "DEV"."PUBLIC"."BUNDLE_ENGAGEMENT_DASHBOARD" AS
with d_metadata as (
    select programid 
        ,  coalesce(partnerseriesid, programid) as content_unit_id
        ,  case when IS_DISNEY_PLUS_ORIGINAL ='TRUE' then 'original' else 'licensed' end as license
        ,  case
                when content_class = 'series' then 'Full Episode'
                when content_class = 'movie' then 'Full Movie'
                else content_class
           end as type
        ,  brands
        ,  max(coalesce(series_full_title,program_full_title)) as content_title
    from "IB_CLEANROOM_SHARE"."HULU_CLEANROOM"."DIM_CONTENT_METADATA"
    group by 1, 2, 3, 4, 5
)
,

subscriber_base as (
    select last_day(to_date(ds),'week') as sunday_date
        ,  count(distinct hulu_user_id) as entitled_base
    from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE"
    where 
        1=1
        and hulu_user_id is not null
        and disney_account_id is not null
        and is_bundle = 1
        and ds between $start_Date and $end_Date
    group by 1
)
,

engagement as (
    select platform
        ,  sunday_date
        ,  coalesce(type, 'All') as content_type
        ,  coalesce(license, 'All') as deal_type
        ,  coalesce(content_title, 'All') as content 
        ,  coalesce(brands, 'All') as brand
        ,  actives
        ,  hours
    from (
        select 'Disney' as platform
            ,  last_day(to_date(calendar_date),'week') as sunday_date
            ,  m.type
            ,  license
            ,  m.content_title
            ,  brands
            ,  count(distinct case when watch_ms >= 10000 then user_id else null end) as actives
            ,  sum(watch_ms / 3600000) as hours
        from content_mart.cleanroom.fact_bundle_engagement_day a
            join "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" hcc 
                on hcc.hulu_user_id = a.user_id
                and hcc.ds = a.calendar_date
            join d_metadata m 
                    on m.programid = a.program_id
        where 
            1=1
            and calendar_date between $start_Date and $end_Date
            and is_bundle = 1
            and hulu_user_id is not null
            and disney_account_id is not null
        group by 1, 2, cube(3, 4, 5, 6)
    )
    
    union all

    select platform
        ,  sunday_date
        ,  coalesce(type, 'All') as content_type
        ,  coalesce(license, 'All') as deal_type
        ,  coalesce(content_title, 'All') as content 
        ,  coalesce(brands, 'All') as brand
        ,  actives
        ,  hours
    from (
        select 'Hulu' as platform
            ,  last_day(to_date(calendar_date),'week') as sunday_date
            ,  programming_type as type
            ,  license_type as license
            ,  content_title
            ,  case
                   when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Hulu Disney Branded TV'
                   when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                   when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                   when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                   when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                   when v.channel in ('FX','FXX') then 'FX'
                   when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                   when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                   else v.channel
               end as brands
            ,  count(distinct case when has_Watched_threshold = 1 then f.subscriber_id else null end) as actives
            ,  sum(playback_Watched_ms / 3600000) as hours
        from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
            join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
                on f.video_id = v.video_id
                and v.programming_type in ('Full Episode', 'Full Movie')
            join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
                on f.content_partner_id = c.content_partner_id
            join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" dbp
                on f.bundle_package_sk = dbp.bundle_package_sk 
                and dbp.content_source_group = 'SVOD'
            join "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" hcc 
                on hcc.hulu_user_id = f.user_id
                and hcc.ds = f.calendar_date
        where 
            1=1
            and calendar_date between $start_Date and $end_Date
            and is_bundle = 1
            and hulu_user_id is not null
            and disney_account_id is not null
        group by 1, 2, cube(3, 4, 5, 6)
    )
)
,

highlevel as (
    select last_day(to_date(ds),'week') as sunday_date
        ,  count(distinct case when (hulu_stream_time_ms >= 10000 or disney_stream_time_ms >= 10000) then hulu_user_id else null end) / count(distinct hulu_user_id) as bundle_Active_Rate
        ,  sum(hulu_stream_time_ms / 3600000) as total_hulu_hours
        ,  sum(disney_stream_time_ms / 3600000) as total_disney_hours
        ,  total_disney_hours  + total_hulu_hours as total_bundle_hours
        ,  sum(hulu_stream_time_ms / 3600000) / count(distinct hulu_user_id) as hulu_hps
        ,  sum(disney_stream_time_ms / 3600000) / count(distinct hulu_user_id) as disney_hps
        ,  total_bundle_hours / count(distinct hulu_user_id) as bundle_hps
    from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" a 
    where 
        1=1
        and ds between $start_Date and $end_Date
        and is_bundle = 1
        and hulu_user_id is not null
        and disney_account_id is not null
    group by 1
)

select platform
    ,  e.sunday_date
    ,  content_type
    ,  deal_type
    ,  content 
    ,  brand
    ,  actives
    ,  hours
    ,  entitled_base
    ,  total_hulu_hours
    ,  total_disney_hours
    ,  hulu_hps
    ,  disney_hps
    ,  bundle_Active_Rate
    ,  total_bundle_hours
    ,  bundle_hps
from engagement e 
    join subscriber_base s 
        on e.sunday_date = s.sunday_date
    join highlevel h 
        on h.sunday_date = e.sunday_date
;



grant select on table "DEV"."PUBLIC"."BUNDLE_ENGAGEMENT_DASHBOARD" to role public;