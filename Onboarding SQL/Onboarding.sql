# Our Fact table captures ALL of our Hulu engagement watched 

--This table captures engagement(playback_watched_ms) at the calendar_Date, video_id, user_id level
-- Ex. 2021-01-01, 1234 , sam, 30minutes
;
select *
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" 
where calendar_date = '2021-01-01'
limit 10
;
--Key Fields in this table are:
calendar_date = date of engagement

video_id = unique identifier for the specific video watched on Hulu 
content_partner_id = unique identifier for the content partner distributor from where the content came from
user_id / subscriber_id = unique identifier for the individual user/subscriber who watched the content 

user_segment = content preference based segment unique to the user/subscriber 
signup_dt = signup date of that subscribers most recent subscription
days_since_signup = days that the subscriber has been entitled

playback_Watched_ms = the viewing time by the subscriber for that video_id in ms 
has_Watched_threshold = a binary flag used to determine whether or not the subscribers watch event is a valid watch event ; 1 or 0 
;




--daily snapshot
select sum(playback_Watched_ms/3600000) as hours
    ,  count(distinct subscriber_id) as num_subscribers
    ,  count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) as num_actives
    ,  count(distinct case when days_since_signup = 0 and has_Watched_threshold = 1 then subscriber_id else null end) as NSTS
    ,  count(distinct video_id) as unique_videos
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" 
where 
    1=1
    and calendar_date = (select max(calendar_date) from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" )

;



select calendar_date
    ,  sum(playback_Watched_ms/3600000) as hours
    ,  count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) as num_actives
    ,  count(distinct case when days_since_signup = 0 then subscriber_id else null end) as NSTS
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" 
where 
    1=1
    and calendar_date between dateadd('day', -6, current_Date() -1) and current_Date() - 1
group by 1
order by 1
;


select user_segment
    ,  sum(playback_Watched_ms/3600000) as hours
    ,  count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) as num_actives
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" 
where 
    1=1
    and calendar_date = current_Date() - 1
group by 1

;


--Key Metrics:
Hours = sum(playback_Watched_ms/3600000) --TOTAL HOURS WATCHED
Actives = count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) --TOTAL VAILD SUBSCRIBERS WATCHED
First Streams = count(distinct case when days_since_signup = 0 then subscriber_id else null end) --TOTAL SUBSCRIBERS ACQUIRED BY CONTENT



;


#Our Dim_Video table is our source for all content metadata
-- This table is at the video_id and can be joinned on the Fact table on video_id = video_id
;
select *
from "UNIVERSE360"."CONTENT"."DIM_VIDEO"
limit 10;


--Key Fields in this table are:
video_id = unique video identifier 

video_title = individual title of video (episode title for shows)
episode_number = episode number for series 
season_number = season_number for series
content_title = main title name  ---- Content Title = The Handmaids Tale, Video Title = 'epISODE 1'

programming_type = series vs film 
video_length = duration of video in ms 
asset_playback_type = live or vod asset 

series_budget_vertical = financial genre rollup
seg_level_a_name = data science genre rollup 

;


select content_title
from "UNIVERSE360"."CONTENT"."DIM_VIDEO"
where 
    1=1
    --and content_title = 'The Handmaid''s Tale'
    and asset_playback_type = 'LIVE'
    and programming_type = 'Full Episode'
group by 1
LIMIT 100
;



--Originals Availability
select content_title
    ,  count(distinct season_number) as num_seasons
    ,  count(distinct season_number||'-'||episode_number) as num_episodes
from "UNIVERSE360"."CONTENT"."DIM_VIDEO"
where 
    1=1
    and content_title in ('The Handmaid''s Tale', 'Palm Springs', 'Ramy', 'PEN15')
    and programming_type in ('Full Episode', 'Full Movie')
group by 1;



--Putting it together

select content_title
    ,  asset_playback_Type
    ,  sum(playback_Watched_ms/3600000) as hours
    ,  count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) as actives
    ,  count(distinct case when days_since_signup = 0 and has_Watched_threshold = 1 then subscriber_id else null end) as nsts
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
    JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        ON f.video_id = v.video_id
where
    1=1
    and content_title = 'It''s Always Sunny in Philadelphia'
    --and asset_playback_Type = 'VOD'
    and programming_Type = 'Full Episode'
    and calendar_Date = (select max(calendar_Date) from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY")
group by 1, 2
;



select content_title
    ,  count(distinct case when has_Watched_threshold = 1 then subscriber_id else null end) as actives
    ,  rank() over (order by actives desc) as active_Rank
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
    JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        ON f.video_id = v.video_id
where
    1=1
    and asset_playback_Type = 'VOD'
    and calendar_Date = (select max(calendar_Date) from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY")
group by 1
qualify active_Rank <= 10

;




# Our subscription_snapshot table contains a daily snapshot of all of our subscribers and their current entitlements
-- This table is at the subscription_id, subscriber_id, snapshot_Date level
-- joins on the fact table on snapshot_Date = calendar_date and subscriber_id = subscriber_id
;

select *
from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT"
where snapshot_Date = current_Date() -1
limit 10;



userid = unique user identifier
subscriber_id = unique subscriber identifier
subscription_id = unique identifier of the subscriber/users subscription 

product_group_name = sash vs noah subscribers 
base_product_group = SVOD or Live entitled subscriber 

snapshot_Date = date 
signup_Datetime_Est = signup date of subscriber 

activity_status_name = current entitlement standing (entitled or not)
promotion status group = paid vs promotion subscriber

user_deduped = latest record of that subsciber for that day 

upper(b.base_program_type) != 'TEST' and b.activity_status_sk != 10 = removes test subscribers 



;



select base_product_group
    ,  product_group_name
    ,  count(distinct subscriber_id) as entitled_subs
    ,  count(distinct case when signup_Date_est = snapshot_Date then subscriber_id else null end) as signups
from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b
where 
    1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD') --mean that you have the ability to access and watch hulu
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    and snapshot_Date = current_Date() - 1
group by 1, 2


;


--FIRST STREAMS


select base_product_group
    ,  content_title
    ,  sum(playback_Watched_ms/3600000) as hours
    ,  count(distinct case when has_Watched_threshold = 1 then f.subscriber_id else null end) as actives
    ,  count(distinct fs.subscriber_id) as first_Streams
from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
    join "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" s
        on s.snapshot_Date = f.calendar_Date
        and f.subscriber_id = s.subscriber_id
    JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        ON f.video_id = v.video_id
    left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
        on fs.calendar_Date = f.calendar_Date
        and fs.subscriber_id = f.subscriber_id
        and fs.video_id = f.video_id
        and fs.is_first_stream_overall = 1 --gives you a subscribers first stream
where
    1=1
    and content_title = 'Snowfall'
    --and base_product_group = 'SVOD'
    and asset_playback_Type = 'VOD'
    and programming_Type = 'Full Episode'
    and f.calendar_Date = (select max(calendar_Date) from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY")
group by 1, 2
;









