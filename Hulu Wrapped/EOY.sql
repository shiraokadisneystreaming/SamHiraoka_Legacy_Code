--TRENDSETTER TITLES
-- CREATE TABLE "DEV"."PUBLIC"."TRENDSETTER_TITLES" (
-- SHOW_TITLES VARCHAR,
-- SEGMENTS VARCHAR,
-- SEASON VARCHAR,
-- NOTES VARCHAR,
-- POSITION VARCHAR
-- )
-- ;


update "DEV"."PUBLIC"."TRENDSETTER_TITLES"
set season = 'S50' WHERE SHOW_TITLES = 'PEN15'
;

 select *
 FROM "DEV"."PUBLIC"."TRENDSETTER_TITLES"
 ;


--step 1: SET DATE FRAME
set start_Date = '2021-01-01';
set end_Date = '2021-12-01';

-- STEP 2: BUILD AGG TABLE AND Remove Kids Profiles
CREATE OR REPLACE TABLE "DEV"."PUBLIC"."SH_YEAR_END_ENGAGEMENT" AS
select realdate
    ,  userid
    ,  subscriber_id
    ,  a.video_id
    ,  dma
    ,  zipcode
    ,  watched
    ,  content_title
    ,  season_number
    ,  episode_number
    ,  programming_type
    ,  series_budget_vertical
    ,  series_budget_subvertical
    ,  license_type
    ,  content_partner_name
    ,  parent_content_partner_name
    ,  seg_level_a_name
    ,  seg_level_b_name
from "CONTENT_MART_SHARE"."DEFAULT"."PLAYBACK_DAY_EST" a
join "CONTENT_MART"."DEFAULT"."VW_USER_PROFILE_LATEST" pr
    on pr.id = a.profile_id
    and pr.is_kids = 0
join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
    on v.video_id = a.video_id
    and v.programming_type in ('Full Episode', 'Full Movie')
    and v.asset_playback_type = 'VOD'
join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
    on a.content_partner_id = c.content_partner_id
join "CONTENT_MART_SHARE"."DEFAULT"."PACKAGE" p 
    on p.package_id = a.package_id
    and p.package_id in (2,14)
where
    1=1
    and realdate between $start_Date and $end_Date
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
;


--STEP 3: BUILD FIRST TO WATCH temp table
--CURRENTLY MISSING:
-- HIT-Monkey
-- Animaniacs
-- Madagascar: A Little Wild
-- Pen15
CREATE OR REPLACE TABLE dev.public.FIRST_TO_WATCH_TEMP as
with metadata as ( --get trendsetter title metadata
    select segments
        ,  content_title 
        ,  season
        ,  case when content_title = 'The Housewife and the Hustler' then 'S1' else 'S' || v.season_number end as season_match
        ,  v.episode_number
        ,  v.video_id
        ,  POSITION
        ,  min(window_start_date) as ep_launch
        ,  max(length / 3600000) as video_length
        ,  min(ep_launch) over (partition by content_title, season_match) as season_launch
    from "UNIVERSE360"."CONTENT"."VAW_HISTORY_DAY_EST_LATEST" a
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
        JOIN "DEV"."PUBLIC"."TRENDSETTER_TITLES" TT 
            on tt.show_titles = v.content_title
    where 
        1=1
        and package_name in ('Plus', 'NOAH SVOD') --VOD content
        and season_match = season
    group by 1, 2, 3, 4, 5, 6, 7
    having min(window_start_date) between $start_Date and $end_Date
    order by 1, 2, 3, 4, 5, 6
)
,

trendsetter_data as ( --get individual video ids for content engagement filter
    select video_id
        ,  video_length
        ,  season_launch
        ,  content_title
    from metadata
    group by 1, 2, 3, 4
)
,

taste_based_segments as ( --get latest taste based segment
    with latest as (
        select userid 
            ,  max(snapshot_Date) as latest_Date
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT"
        where   
            1=1
        group by 1
    )
    ,

    latest_segment as (
        select us.userid
            ,  us.segment
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT" us 
            join latest l 
                on l.userid = us.userid
                and l.latest_Date = us.snapshot_Date
            left join dev.public.vip_taste_based_segments_11_2021 vip 
                on vip.userid =  us.userid
        where 
            1=1
            and vip.userid is null
        group by 1, 2
    )

    select * from latest_segment

    union all

    select * from dev.public.vip_taste_based_segments_11_2021 vip 
)
,

user_watch as (
    select segment --find the earliest day since content launch in which the user watched more than 80% of a video length
        ,  userid
        ,  content_title
        ,  video_id
        ,  days_since_launch
        ,  case when cume_hours >= (.8 * video_length) then 1 else 0 end as watch_flag
        ,  min(case when watch_flag = 1 then days_since_launch else 500 end) over (partition by userid, content_title) as first_watch --500 is an arbitrary number to inculde people who started but never watched an ep
    from (
        select segment --get all users daily playback for trendsetter content and get running sum of hours
            ,  a.content_title
            ,  a.userid 
            ,  realdate
            ,  a.video_id
            ,  video_length
            ,  season_launch
            ,  datediff('day', season_launch, realdate) + 1 as days_since_launch
            ,  sum(watched / 3600000) as hours_watched
            ,  sum(hours_watched) over (partition by segment, a.userid, a.video_id order by realdate rows between unbounded preceding and current row) as cume_hours
        from "DEV"."PUBLIC"."SH_YEAR_END_ENGAGEMENT" a 
            join taste_based_segments ts 
                on ts.userid = a.userid
            join trendsetter_data td --filter for only videos in specific seasons
                on td.video_id = a.video_id
        where
            1=1
        --    and a.userid in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
        group by 1, 2, 3, 4, 5, 6, 7, 8
    )
    where 
        1=1
    group by 1, 2, 3, 4, 5, 6
)

select segment --flatten users list
    ,  userid
    ,  max(case when position = '1' then content_title || '-' || season else null end) as title_1
    ,  max(case when position = '2' then content_title || '-' || season else null end) as title_2
    ,  max(case when position = '3' then content_title || '-' || season else null end) as title_3
//    ,  max(case when position = '1' and bucket between 1 and 4 then 1 else 0 end) as title_1_first_to_watch
//    ,  max(case when position = '2' and bucket between 1 and 4 then 1 else 0 end) as title_2_first_to_watch
//    ,  max(case when position = '3' and bucket between 1 and 4 then 1 else 0 end) as title_3_first_to_watch
     ,  max(case when position = '1' then bucket end) as title_1_first_to_watch -- 20th percentile
     ,  max(case when position = '2' then bucket end) as title_2_first_to_watch
     ,  max(case when position = '3' then bucket end) as title_3_first_to_watch 
from (
    select segment --assign users to buckets based on when they watched a piece of content
        ,  u.content_title
        ,  season
        ,  u.userid
        ,  POSITION
        ,  first_watch
        ,  ntile(10) over (partition by segment, u.content_title, season order by first_watch) as bucket
    from user_watch u 
        join metadata m --limit segment users to segment shows
            on u.segment  = m.segments 
            and u.video_id = m.video_id
    where 
        1=1
    group by 1, 2, 3, 4, 5, 6
)
where 
    1=1
group by 1, 2
;




----USER LEVEL DATA output
CREATE OR REPLACE TABLE DEV.PUBLIC.EOY_API_USER_TABLE_DEC AS
with taste_based_segments as ( --get latest taste based segment
    with latest as (
        select userid 
            ,  max(snapshot_Date) as latest_Date
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT"
        where   
            1=1
        group by 1
    )
    ,

    latest_segment as (
        select us.userid
            ,  us.segment
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT" us 
            join latest l 
                on l.userid = us.userid
                and l.latest_Date = us.snapshot_Date
            left join dev.public.vip_taste_based_segments_11_2021 vip 
                on vip.userid =  us.userid
        where 
            1=1
            and vip.userid is null
        group by 1, 2
    )

    select * from latest_segment

    union all

    select * from dev.public.vip_taste_based_segments_11_2021 vip 
)
, 

user_name as ( --get available account holder name
    select user_id as userid
        ,  name
    from "CONTENT_MART"."DEFAULT"."METADATA_USER_PROFILE"
    where 
        1=1
        and is_master = 1
        --and user_id in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
        and realdate = $end_Date
    group by 1, 2
)
,

 metadata as ( --get video lengths
    select content_title 
        ,  v.season_number
        ,  v.episode_number
        ,  v.video_id
        ,  max(length / 3600000) as video_length
    from "UNIVERSE360"."CONTENT"."VAW_HISTORY_DAY_EST_LATEST" a
        join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
            on a.content_partner_id = c.content_partner_id
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
    where 
        1=1
        and package_name in ('Plus', 'NOAH SVOD') --VOD content
        and window_start_date <= $end_Date
        AND window_end_Date >= $start_Date
    group by 1, 2, 3, 4
)
//  select * from metadata 
//  where 
//    (video_length = 0 
//  or video_length is null)
,

 sub_Genre_list as ( --for cross join in next step
    select 'fake' as fake_join
        ,  seg_level_b_name
    from "UNIVERSE360"."CONTENT"."DIM_VIDEO"
    group by 1, 2
)
,

user_sub_genre_agg_engagement as ( --get users sub genre pcts
    select userid
        ,  seg_level_b_name
        ,  b_hours / nullif(total_user_hours,0) as pct_hours --division by zero
    from(
        select userid
            ,  s.seg_level_b_name
            ,  sum(case when a.seg_level_b_name = s.seg_level_b_name then seg_b_level_hours else 0 end) as b_hours
            ,  sum(b_hours) over (partition by userid) as total_user_hours
        from (
            select userid 
                ,  'fake' as fake_join
                ,  seg_level_b_name
                ,  sum(watched / 3600000) as seg_b_level_hours
            from "DEV"."PUBLIC"."SH_YEAR_END_ENGAGEMENT"
            where
                1=1
          --      and userid in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
            group by 1, 2, 3
        ) a 
        join sub_Genre_list s  
            on a.fake_join =   s.fake_join
        group by 1, 2
    )
)
,

user_content_count as (  --get number of titles watched for each user
    select userid
        ,  count(distinct case when programming_type = 'Full Episode' and ((hours / nullif(video_length,0)) >= .8) then content_title else null end) as show_count
        ,  count(distinct case when programming_type = 'Full Movie' and ((hours / nullif(video_length,0)) >= .8) then content_title else null end) as movie_count
        ,  count(distinct case when license_type = 'original' and ((hours / nullif(video_length,0)) >= .8) then content_title else null end) as original_count
    from (
        select userid 
            ,  programming_type
            ,  license_type
            ,  m.content_title
            ,  m.video_id
            ,  video_length
            ,  sum(watched / 3600000) as hours
        from "DEV"."PUBLIC"."SH_YEAR_END_ENGAGEMENT" a 
            join metadata m 
                on a.video_id = m.video_id
        where
            1=1
       --     and userid in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
        group by 1, 2, 3, 4, 5, 6
    )
    group by 1
)
,

top_Watched_month as (  --get users top watched month
    select userid 
        ,  date_trunc('month', realdate) as top_watched_month
        ,  sum(watched / 3600000) as hours
        ,  row_number() over (partition by userid order by hours desc) as top_month
    from "DEV"."PUBLIC"."SH_YEAR_END_ENGAGEMENT" a 
    where
        1=1
      --  and userid in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
    group by 1, 2
    qualify top_month = 1 and hours > 0
)

--left join it all to preserve users who didnt engage or are missing anything
select ussl.userid
    ,  ussl.subscriber_id
    ,  ts.segment
    ,  name as first_name
    ,  show_count as total_num_show_titles_watched
    ,  movie_count as total_num_movie_titles_watched
    ,  original_count as total_num_original_titles_watched
    ,  to_varchar(top_watched_month, 'yyyy-mm-dd')  as max_month
    ,  title_1 as user_segments_show_1_name
    ,  title_2 as user_segments_show_2_name
    ,  title_3 as user_segments_show_3_name
    ,  title_1_first_to_watch as top_pct_to_watch_segment_show_1_first
    ,  title_2_first_to_watch as top_pct_to_watch_segment_show_2_first
    ,  title_3_first_to_watch as top_pct_to_watch_segment_show_3_first
    ,  max(case when seg_level_b_name =  'B Adult Animation' then pct_hours else null end) as pct_adult_animation
    ,  max(case when seg_level_b_name =  'B TV: Sitcoms' then pct_hours else null end) as pct_tv_sitcoms
    ,  max(case when seg_level_b_name =  'B TV: Emotional Dramas' then pct_hours else null end) as pct_tv_emotional_dramas
    ,  max(case when seg_level_b_name =  'B High Stakes Dramas' then pct_hours else null end) as pct_high_stakes_drama
    ,  max(case when seg_level_b_name =  'B TV: Crime Dramas' then pct_hours else null end) as pct_tv_crime_dramas
    ,  max(case when seg_level_b_name =  'B TV: Reality' then pct_hours else null end) as pct_tv_reality
    ,  max(case when seg_level_b_name =  'B Kids: Older Elementary' then pct_hours else null end) as pct_kids_older_elementary
    ,  max(case when seg_level_b_name =  'B Anime' then pct_hours else null end) pct_anime
    ,  max(case when seg_level_b_name =  'B Documentaries' then pct_hours else null end) as pct_documentaries
    ,  max(case when seg_level_b_name =  'B TV: Sci-Fi' then pct_hours else null end) as pct_tv_sci_fi
    ,  max(case when seg_level_b_name =  'B TV: Food and Home' then pct_hours else null end) as pct_tv_food_and_home
    ,  max(case when seg_level_b_name =  'B TV: Edgy Comedies' then pct_hours else null end) as pct_tv_edgy_comedies
    ,  max(case when seg_level_b_name =  'B TV: Competition' then pct_hours else null end) as pct_tv_competition
    ,  max(case when seg_level_b_name =  'B Young Adult/Romance' then pct_hours else null end) as pct_young_adult_romance
    ,  max(case when seg_level_b_name =  'B TV: Unscripted General Interest' then pct_hours else null end) as pct_tv_unscripted_general_interest
    ,  max(case when seg_level_b_name =  'B Kids: Younger Elementary' then pct_hours else null end) as pct_kids_younger_elementary
    ,  max(case when seg_level_b_name =  'B Daytime & News' then pct_hours else null end) as pct_daytime_and_news
    ,  max(case when seg_level_b_name =  'B TV: International' then pct_hours else null end) as pct_tv_international
    ,  max(case when seg_level_b_name =  'B Sports' then pct_hours else null end) as pct_sports
    ,  max(case when seg_level_b_name =  'B Spanish Language' then pct_hours else null end) as pct_spanish_language
    ,  max(case when seg_level_b_name =  'B Musicals & Specials' then pct_hours else null end) as pct_musicals_specials
    ,  max(case when seg_level_b_name =  'B Film: LGBT' then pct_hours else null end) as pct_film_lgbtq
    ,  max(case when seg_level_b_name =  'B TV: Asian Dramas' then pct_hours else null end) as pct_tv_asian_dramas
    ,  max(case when seg_level_b_name =  'B Film: Action' then pct_hours else null end) as pct_film_action
    ,  max(case when seg_level_b_name =  'B Film: Comedy' then pct_hours else null end) as pct_film_comedy
    ,  max(case when seg_level_b_name =  'B Horror' then pct_hours else null end) as pct_horror
    ,  max(case when seg_level_b_name =  'B Film: Dramas' then pct_hours else null end) as pct_film_dramas
    ,  max(case when seg_level_b_name =  'B Film: Sci-Fi' then pct_hours else null end) as pct_film_scifi
    ,  max(case when seg_level_b_name =  'N/A' then pct_hours else null end) as pct_na --capture SEG_LEVEL_B_NAME='N/A'
    ,  max(case when seg_level_b_name =  'B Film: Thriller' then pct_hours else null end) as pct_film_thriller
from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" ussl /** make sure latest date matches date of running **/
    left join taste_based_segments ts 
        on ts.userid = ussl.userid
    left join user_name un      
        on un.userid = ussl.userid
    left join user_sub_genre_agg_engagement gae 
        on gae. userid = ussl.userid
    left join user_content_count ucc 
        on ucc.userid = ussl.userid
    left join top_Watched_month twm 
        on twm.userid = ussl.userid
    left join dev.public.FIRST_TO_WATCH_TEMP FTW 
        on ftw.userid = ussl.userid
where
    1=1
  --  and ussl.userid in ('151893364', '106237364', '113227224', '105933596', '172728777', '109085086')
    and ussl.snapshot_date = $end_Date
    and ussl.user_deduped = 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

;











select segment
    ,  user_segments_show_1_name
    ,  user_segments_show_2_name
    ,  user_segments_show_3_name
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC
where 
    1=1
group by 1, 2, 3, 4


;

select count(*) -- 122900130

        ,count(distinct userid)--122900130

        , count(distinct subscriber_id)--122900130

        // show
        , avg(total_num_show_titles_watched)--19.299821

        
        , avg(total_num_movie_titles_watched)--12.487480
        
        , avg(total_num_original_titles_watched)--3.28602

from DEV.PUBLIC.EOY_API_USER_TABLE_DEC

;

select ussl.userid
    ,  ussl.segment
    ,  ftw.*
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC ussl
    left join dev.public.FIRST_TO_WATCH_TEMP FTW 
        on ftw.userid = ussl.userid
where ussl.segment <> ftw.segment
limit 100
;



select segment, count(*)
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC
group by 1

;


 
 
 select *
 from dev.public.prod_test_api_user_table
 ;
 
 
 
 
 
 grant select on table DEV.PUBLIC.EOY_API_USER_TABLE_DEC to role MI_ADMIN;
 
 
 
 
select segment
        , count(distinct a.userid) as users
        , users / sum(users) over () as pct_total
 from DEV.PUBLIC.EOY_API_USER_TABLE_DEC a
    join "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b
        on a.userid = b.userid
        and b.snapshot_Date = '2021-12-01'
        and b.user_deduped = 1
where activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
group by 1
;


select *
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC a
limit 100
--where userid in ('191629446', '152742844')




;
select userid, count(*)
from dev.public.FIRST_TO_WATCH_TEMP a
group by 1
having count(*) > 1
;

select * 
from dev.public.vip_taste_based_segments_11_2021
where userid = '132723026';





with taste_based_segments as ( --get latest taste based segment
    with latest as (
        select userid 
            ,  max(snapshot_Date) as latest_Date
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT"
        where   
            1=1
        group by 1
    )
    ,

    latest_segment as (
        select us.userid
            ,  us.segment
        from "DEV"."PUBLIC"."TASTE_BASED_SEGMENTATION_USER_SNAPSHOT" us 
            join latest l 
                on l.userid = us.userid
                and l.latest_Date = us.snapshot_Date
            left join dev.public.vip_taste_based_segments_11_2021 vip 
                on vip.userid =  us.userid
        where 
            1=1
            and vip.userid is null
        group by 1, 2
    )

    select * from latest_segment

    union all

    select * from dev.public.vip_taste_based_segments_11_2021 vip 
)


select segment
        , count(distinct b.userid) as users
        , users / sum(users) over () as pct_total
 from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b
    left join taste_based_segments t 
        on t.userid = b.userid
where 
    1=1
    and activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.user_deduped = 1
    and b.snapshot_Date = '2021-12-01'
group by 1
;




 select * from dev.public.vip_taste_based_segments_11_2021 vip 
 where userid = '149345204'
 
 
 
 ;
 
 
 



select distinct promotion_status_group
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT_LATEST" b

;



select count(*)
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC



;





CREATE OR REPLACE TABLE DEV.PUBLIC.EOY_API_USER_TABLE_DEC_REDUCED AS
with subs as (
    select userid
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b
    where 
        1=1
        and activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
        and b.user_deduped = 1
        and b.snapshot_Date between '2021-01-01' and '2021-12-01'
    group by 1
)


select a.*
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC a 
    join subs s 
        on s.userid = a.userid
        ;
        
select count(*)   
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC_REDUCED;


 grant select on table DEV.PUBLIC.EOY_API_USER_TABLE_DEC_REDUCED to role MI_ADMIN;
 
 
 
 select count(*)   
from DEV.PUBLIC.EOY_API_USER_TABLE_DEC