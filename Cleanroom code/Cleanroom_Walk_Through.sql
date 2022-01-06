INTRODUCTION:

The Cleanroom is a system put together by Data Eng that allows us to pull in Disney+ engagement for subscribers who HAVE 
registered a Hulu account. The only condition that we check for is that the disney account email matches an email in our hulu 
registered_user table.

By using the cleanroom, we can look at cross-platform engagement to understand how our bundle subscribers are using both platforms 
and what titles are driving performance.

To access the tables, you must get the role DB_CLEANROOM_READER - reach out to Makda Haile


----------------------------------------------------------------------------------
USE CASES 

The bundle engagement use cases can be answered by 2 main sets of table:

The first table, "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE", allows us to:
    1. Identify and tag subscribers by their entitlements
    2. Sum platform level hours (D+, Hulu, ESPN)
    3. Count actives and entitled subscribers by platform or subscriber tagging 
    4. HPS/Actives/Hour share across all platforms as well as counts of entitled subscribers 

And our main subscriber entitlement groupings are currently : 
    1. Hulu Standalone: Has a single Hulu entitlement only 
    2. Disney Standlone: Has a single D+ entitlement only 
    3. Dual Standalone: Has Hulu and Disney entitlement via individual subscriptions
    4. Bundle subcriber: Has all entitlements via a bundle subscription 


The second table, "content_mart"."cleanroom"."fact_bundle_engagement_day", allows us to:
    1. Look at daily D+ engagement at the program_id level for subscribers who have a registered hulu account 
    2. Get D+ title level hours and actives 

----------------------------------------------------------------------------------
Table Structure: "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE"

--Summary
Table: "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE"
High Level Engagement and subscription status for All Subscribers:
Captures all Disney Streaming Subscribers at the daily level and checks their platform entitlements as well as their platform streaming time
Table is updated daily (DS) and contains an email_hash, hulu_user_id, disney_account_id, and espn_account_id for each user 
Jake Elter can be a point of contact for this table
;
select *
from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE"
where ds = '2021-01-01'
limit 10;
*** always set your DS date as table is massive

--Fields
Can determine a subscribers subscription state: bundle, standalone, dual standalone, etc...

    Bundle Entitlement fields: is_bundle = 1 and is_entitled_bundle = 1
    Hulu Entitlement Fields: is_hulu_standalone = 1 and hulu_is_entitled = 1
    Disney Entitlement Fields: is_disney_standalone = 1 and disney_is_entitled = 1

    Code Example:
        max(case when is_bundle = 1 and is_entitled_bundle = 1 then 1 else 0 end) as bundle
        max(case when is_hulu_standalone = 1 and hulu_is_entitled = 1 and is_disney_standalone = 0 and is_bundle = 0 then 1 else 0 end) as hulu_only
        max(case when is_hulu_standalone = 1 and is_disney_standalone = 1 and hulu_is_entitled = 1 and disney_is_entitled = 1 and is_bundle = 0 then 1 else 0 end) as dual_standalone
;
        select email_hash
        ,  hulu_user_id
        ,  disney_account_id
        ,  max(case when is_bundle = 1 and is_entitled_bundle = 1 then 1 else 0 end) as bundle
        ,  max(case when is_hulu_standalone = 1 and hulu_is_entitled = 1 and is_disney_standalone = 0 and is_bundle = 0 then 1 else 0 end) as hulu_only
        ,  max(case when is_hulu_standalone = 0 and is_disney_standalone = 1 and disney_is_entitled = 1 and is_bundle = 0 then 1 else 0 end) as Disney_only
        ,  max(case when is_hulu_standalone = 1 and is_disney_standalone = 1 and hulu_is_entitled = 1 and disney_is_entitled = 1 and is_bundle = 0 then 1 else 0 end) as dual_standalone
    from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" cc
    where 
        1=1
        and ds = '2021-01-01'
    group by 1, 2, 3
    limit 10
;

Can also calculate streaming time per user per day per platform
    Disney Streaming Time: sum(disney_stream_time_ms / 3600000)
    Hulu Streaming Time: sum(hulu_stream_time_ms / 3600000)
    ESPN Streaming Time: sum(espn_stream_time_ms / 3600000)



--Example Code
Subscriber Tagging and subscriber count;
with subscriber_tagging as (
    select email_hash
        ,  max(case when is_bundle = 1 and is_entitled_bundle = 1 then 1 else 0 end) as bundle
        ,  max(case when is_hulu_standalone = 1 and hulu_is_entitled = 1 and is_disney_standalone = 0 and is_bundle = 0 then 1 else 0 end) as hulu_only
        ,  max(case when is_hulu_standalone = 0 and is_disney_standalone = 1 and disney_is_entitled = 1 and is_bundle = 0 then 1 else 0 end) as Disney_only
        ,  max(case when is_hulu_standalone = 1 and is_disney_standalone = 1 and hulu_is_entitled = 1 and disney_is_entitled = 1 and is_bundle = 0 then 1 else 0 end) as dual_standalone
    from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" cc
    where 
        1=1
        and ds = '2021-09-30'
    group by 1
)

select 'bundle' as type
    ,  count(distinct st.email_hash) as sub_count
from subscriber_tagging st 
where
    1=1
    and bundle = 1
group by 1

union all

select 'dual standalone' as type
    ,  count(distinct st.email_hash) as sub_count
from subscriber_tagging st 
where
    1=1
    and dual_standalone = 1
group by 1

union all

select 'hulu only' as type
    ,  count(distinct st.email_hash) as sub_count
from subscriber_tagging st 
where
    1=1
    and hulu_only = 1
group by 1

union all

select 'Disney only' as type
    ,  count(distinct st.email_hash) as sub_count
from subscriber_tagging st 
where
    1=1
    and disney_only = 1
group by 1

;


HPS:;
select date_trunc('week', ds) as week
    ,  sum(hulu_stream_time_ms / 3600000) / count(distinct hulu_user_id) as hulu_hps
    ,  sum(disney_stream_time_ms / 3600000) / count(distinct hulu_user_id) as disney_hps
    ,  sum(espn_stream_time_ms / 3600000) / count(distinct hulu_user_id) as espn_hps
    ,  count(distinct hulu_user_id) as bundle_count
from "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" a 
where 
    1=1
    and ds = '2021-12-01'
    and is_bundle = 1 and is_entitled_bundle = 1
    --and hulu_user_id is not null
group by 1

;
*** Something to note is that YOU dictate the lense in which you run your analysis and calculations -
    whats meant by this is that not all users have hulu_user_id so if you count(distinct hulu_user_id)
    then you only capture subscribers who have a hulu account ... this will differ than using email_hash
    which everyone has. Keep in mind paritally completed accounts.

----------------------------------------------------------------------------------------------------
Table: "content_mart"."cleanroom"."fact_bundle_engagement_day"

--Summary
Table: "content_mart"."cleanroom"."fact_bundle_engagement_day"
This table pulls in daily engagement at the program_id level from the D+ fact engagement table for Disney Users who have a Hulu User Id 
Table updates daily 
;
SELECT *
FROM content_mart.cleanroom.fact_bundle_engagement_day
WHERE CALENDAR_DATE = '2021-01-01'
limit 10;

--Fields
User_ID = this is the bundle users Hulu user_id and can be joined to the HULU_DSS_CONFORMED_COMPOSITE table on user_id = hulu_user_id or can be joined to any hulu table on user_id = userid 
Calendar_Date = date of watch event
Content_Title = parent content name
watch_ms = watch time in ms 
program_id and content_unit_id come from D+ table 


;
--Example Query
SELECT content_title
    ,  count(distinct case when watch_ms >= 10000 then user_id else null end) as actives
FROM content_mart.cleanroom.fact_bundle_engagement_day
WHERE CALENDAR_DATE = '2021-01-01'
group by 1
order by 2 desc 
limit 100

;

We can also union or left join this table to "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY"
to stack rank Disney Streaming titles or hours :

Example Code:;
with engagement as (
    select 'Disney' as platform
        ,  content_title
        ,  count(distinct case when watch_ms > 10000 then a.user_id else null end) as actives
        ,  sum(watch_ms / 3600000) as hours
    from content_mart.cleanroom.fact_bundle_engagement_day a
        join "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" hcc 
            on hcc.hulu_user_id = a.user_id
            and hcc.ds = a.calendar_date
    where 
        1=1
        and calendar_Date = '2021-12-01'
        and (is_goodstanding_bundle = 1 or is_grace_bundle = 1)
    group by 1, 2
    
    union all  
    
    select 'Hulu' as platform
        ,  content_title
        ,  count(distinct case when has_Watched_threshold = 1 then f.subscriber_id else null end) as actives
        ,  sum(playback_Watched_ms / 3600000) as hours
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on f.video_id = v.video_id
            and v.programming_type in ('Full Episode', 'Full Movie')
        join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" dbp
            on f.bundle_package_sk = dbp.bundle_package_sk 
            and dbp.content_source_group = 'SVOD'
        join "IB_DSS_SHARE"."HULU"."HULU_DSS_CONFORMED_COMPOSITE" hcc 
            on hcc.hulu_user_id = f.user_id
            and hcc.ds = f.calendar_date
    where 
        1=1
        and calendar_date = '2021-12-01'
        and (is_goodstanding_bundle = 1 or is_grace_bundle = 1)
    group by 1, 2
)

select *
from engagement
order by actives desc limit 20
;


Furthermore, we have also pulled in the D+ metadata table which updates daily:
Table: "IB_CLEANROOM_SHARE"."HULU_CLEANROOM"."DIM_CONTENT_METADATA"
This table can be joined on program_id to pull in brands and other info

--------------------------------------------------------------------------------
TASTE-BASED USER SEGMENTS:

Hulu user taste-based segments table: "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT"
Code for latest user segment tagging:

taste_based_segments as ( --get users latest segment
    with latest as (
        select userid 
            ,  max(snapshot_Date) as latest_Date
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT"
        where   
            1=1
        group by 1
    )
    
    select us.userid as user_id
        ,  segment
    from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT" us 
        join latest l 
            on l.userid = us.userid
            and l.latest_Date = us.snapshot_Date
    group by 1, 2
)

Disney User taste-based segment table:"IB_CLEANROOM_SHARE"."HULU_CLEANROOM"."VW_DIM_DISNEY_US_SURVEY_TASTE_BASED_SEGMENTS_SHARE"
*** this is currently a one time snapshot as of 12/1/2021 so please reach out to @chris sword or Lian Jian for an updated pull