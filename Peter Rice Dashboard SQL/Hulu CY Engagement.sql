set max_Date = '2021-11-30';


CREATE OR REPLACE TABLE "DEV"."PUBLIC"."CURRENT_CALENDAR_YEAR_CONTENT_AGG" AS
with dates as (
    select calendar_date
    from "UNIVERSE360"."CONTENT"."DIM_DATE"
    where 
        1 = 1
        and calendar_date between date_trunc('year', current_date()) and $max_Date
    group by 1
)
,

metadata as (
    select coalesce(month_Date, last_day(current_Date, 'year')) as month 
        ,  is_original
        ,  vertical
        ,  content_title
        ,  programming_type
        ,  sum(hours) as avail_hours
    from(
        select date_trunc('month', to_Date(d.calendar_date)) as month_Date
            ,  case 
                    when (license_type = 'original' or v.channel = 'Hulu Original Series') then 'Original' 
                    else 'Non Original'
               end as is_original
            ,  series_budget_vertical as vertical
            ,  v.programming_type
            ,  v.content_title
            ,  v.content_title || '-' || v.season_number || '-' || v.episode_number as video
            ,  max(video_length / 3600000) as hours
        from "UNIVERSE360"."CONTENT"."VAW_HISTORY_DAY_EST_LATEST" a
            join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
                on a.content_partner_id = c.content_partner_id
            join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
                on a.video_id = v.video_id
                and v.programming_type in ('Full Episode','Full Movie')
                and v.asset_playback_type = 'VOD'
            join dates d 
                on d.calendar_date between window_start_Date and window_end_Date
        where 
            1=1
            and package_name in ('Plus', 'NOAH SVOD') --VOD content
            and window_start_Date <= $max_Date
            and window_end_Date >= date_trunc('year', current_date())
            --and content_title = 'Only Murders in the Building'
        group by cube(1), 2, 3, 4, 5, 6
    )
    group by 1, 2, 3, 4, 5
)
,

engagement as (  
    select coalesce(month_Date, last_day(current_Date, 'year')) as month 
        ,  vertical
        ,  is_original
        ,  content_title
        ,  programming_type
        ,  actives
        ,  first_streams
        ,  hours
    from(
        SELECT date_trunc('month', pb.calendar_date) as month_date
            ,  series_budget_vertical as vertical
            ,  case 
                    when (license_type = 'original' or channel = 'Hulu Original Series') then 'Original' 
                    else 'Non Original'
                end as is_original
            ,  content_title
            ,  vid.programming_type
            ,  count(distinct case when has_Watched_threshold = 1 then pb.subscriber_id else null end) as actives
            ,  count(distinct fs.subscriber_id) as first_streams
            ,  SUM(pb.playback_watched_ms / 3600000) AS hours
        FROM UNIVERSE360.CONTENT.FACT_USER_CONTENT_CONSUMPTION_DAY AS pb
                INNER JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" AS vid
                    ON vid.video_id = pb.video_id
                    AND UPPER(vid.programming_type) IN ('FULL EPISODE', 'FULL MOVIE')
                    AND vid.asset_playback_type = 'VOD'
                INNER JOIN UNIVERSE360.CONTENT.DIM_BUNDLE_PACKAGE AS bdl
                    ON bdl.bundle_package_sk = pb.bundle_package_sk
                    AND bdl.content_source_group = 'SVOD'
                INNER JOIN "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" AS cp 
                    ON cp.content_partner_id = pb.content_partner_id
                left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
                    on fs.calendar_Date = pb.calendar_Date
                    and fs.subscriber_id = pb.subscriber_id
                    and fs.video_id = pb.video_id
                    and fs.is_first_stream_overall = 1
        WHERE 
            1=1
            and pb.calendar_date between date_trunc('year', current_date()) and $max_Date
        GROUP BY cube(1), 2, 3, 4, 5
    )
)
,

engaegment_rollup as (
    select month
        ,  vertical
        ,  is_original
        ,  content_title
        ,  programming_type
        ,  actives
        ,  first_streams
        ,  hours
        ,  sum(first_streams) over (partition by month) as monthly_first_stream
        ,  sum(hours) over (partition by month) as monthly_hours

        ,  max(actives) over (partition by vertical, programming_type, is_original, content_title) as max_actives
        ,  max(hours) over (partition by vertical, programming_type, is_original, content_title) as max_hours
        ,  max(first_streams) over (partition by vertical, programming_type, is_original, content_title) as max_first_streams
    from engagement
)
,

entitled_subs as (
    select coalesce(month_Date, last_day(current_Date, 'year')) as month 
        ,  sub_base
    from(
        SELECT date_trunc('month', to_date(snapshot_Date)) as month_Date
            ,  count(distinct subscriber_id) as sub_base
        from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b   --live user filter
        where 
            1=1
            and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD') --entitled
            and b.promotion_status_group IN ('PAID', 'PROMOTION') --via a paid or promotion channel
            and b.activity_status_sk != 10 --removes wholesale test
            and upper(b.base_program_type) != 'TEST' --removes test users
            and b.user_deduped = 1 --latest status of that day
            and snapshot_Date between date_trunc('year', current_date()) and $max_Date --filter for date
        group by cube(1)
    )
)


select er.month
    ,  er.vertical
    ,  er.is_original
    ,  er.content_title
    ,  er.programming_type
    ,  avail_hours

    ,  actives
    ,  actives / nullif(sub_base, 0) as reach
    ,  actives / nullif(max_actives, 0) as pct_yearly_title_Actives

    ,  first_streams
    ,  first_streams / nullif(monthly_first_stream, 0) as pct_monthy_first_streams
    ,  first_streams / nullif(max_first_streams, 0) as pct_yearly_title_first_streams

    ,  hours 
    ,  hours / nullif(monthly_hours, 0) as pct_monthly_hours
    ,  hours / nullif(max_hours, 0) as pct_yearly_title_hours
    ,  hours / nullif(avail_hours, 0) as volume_efficiency

from engaegment_rollup er 
    join entitled_subs es 
        on es.month = er.month
    join metadata m 
        on m.month = er.month 
        and m.vertical = er.vertical
        and m.is_original = er.is_original
        and m.content_title = er.content_title
        and m.programming_type = er.programming_type
where 
    1=1
    --and er.content_title = 'Only Murders in the Building'
order by 4, 1;




