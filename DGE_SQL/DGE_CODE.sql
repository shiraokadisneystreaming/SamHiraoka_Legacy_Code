--- Ok, there are about 6 or so create table queries in here but i promise they are not THAT gnarly....
-- These are good to rerun though and make sure to make any code changees to the harmony jobs as well which can be searched for "dge_"
-- if you need to rerun tables, i would suggest making moving the timefrrame to only be the last year
-- only hardcoded date thing in here is the start date '2020-03-02' cuz thats the fx launch



CREATE OR REPLACE TABLE "DEV"."PUBLIC"."NETWORK_LEVEL_ENGAGEMENT" AS

with last_sunday_Date as ( --get latest sunday
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)
,

dates as  --get all sundays
( 
    select
    weekenddate as week
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where realdate between '2020-03-02' and (select * from last_sunday_Date)
    group by 1
)
,


video_id_license_status as -- get all status
(
    select
    last_day(to_date(calendar_date),'week') as sunday_date,
    content_title,
    a.video_id,
    cla_current_status,
    content_partner_name,
    channel,
    license_type,
    film_window_type
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
    join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        on a.video_id = v.video_id
        and v.programming_type in ('Full Episode','Full Movie')
    left join "UNIVERSE360"."CONTENT"."DIM_VIDEO_FILM_WINDOW" vw 
        on vw.video_id = a.video_id
        and a.calendar_Date between film_window_start_date and film_window_end_Date
    join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
        on a.content_partner_id = c.content_partner_id
    join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
        on a.bundle_package_sk = p.bundle_package_sk 
        and p.content_source_group = 'SVOD'
    where 1=1
    and calendar_date between '2020-03-02' and (select * from last_sunday_Date)
    group by 1,2,3,4,5,6,7,8
)
,

content_license_grouping as ( --assign status to video
    select
    *
    from(
        select
        v.sunday_date,
        v.content_title,
        v.video_id,
        case
            when film_window_type is not null then film_window_type
            when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and license_type <> 'original' and channel not in ('FX','FXX'))) then 'Current'
            when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 'Exclusives'
            when v.cla_current_status = 'current_prior' then 'Prior' 
            else 'Library'
        end as content_type,
        case
            when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and license_type <> 'original' and channel not in ('FX','FXX'))) then 1
            when v.cla_current_status = 'current_prior' then 2
            when film_window_type is not null then 3
            when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 4
            else 5
        end as content_type_rank,
        min(content_type_rank) over (partition by v.sunday_date, v.content_title, v.video_id) as rank_use
        from video_id_license_status v
        where 1 = 1
        group by 1,2,3,4,5
        order by 1,2
    )
    where rank_use = content_type_rank
)
,

metadata as ( --roll up metadata
    select
    week as sunday_date,
    network,
    coalesce(content_type, 'Total') as cla_status,
    count(distinct content_title) as avail_titles,
    sum(episodes) as avail_episodes,
    sum(hours) as avail_hours,
    sum(case when cla_status = 'Total' then avail_titles else 0 end) over (partition by sunday_Date) as total_titles,
    sum(case when cla_status = 'Total' then avail_episodes else 0 end) over (partition by sunday_Date) as total_episodes,
    sum(case when cla_status = 'Total' then avail_hours else 0 end) over (partition by sunday_Date) as total_hours
    from
    (
        select
        week,
        network,
        content_title,
        content_type,
        count(distinct season_number || ' - ' || episode_number) as episodes,
        sum(hours) as hours
        from 
        (
            select
            d.week,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
            v.content_title,
            cl.content_type,
            v.season_number,
            v.episode_number,
            max(video_length)/3600000 as hours
            from UNIVERSE360.CONTENT.VAW_HISTORY_DAY_EST_LATEST a
            join UNIVERSE360.CONTENT.DIM_CONTENT_PARTNER c 
              on a.content_partner_id = c.content_partner_id
            join UNIVERSE360.CONTENT.DIM_VIDEO v 
              on a.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join dates d 
              on window_start_date <= d.week and window_end_date >= dateadd('day',-6,d.week)
            join content_license_grouping cl 
              on cl.sunday_date = d.week
              and cl.video_id = a.video_id
            where 1=1     
            and package_name in ('NOAH SVOD','Plus')
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
            group by 1,2,3,4,5,6
        ) a
        group by 1,2,3,cube(4)
    )
    group by 1,2,3
)
,

sub_base as --get entitled subs in week
( 
    select
    last_day(to_date(snapshot_date),'week') as sunday_date,
    count(distinct subscriber_id) as entitled_subs,
    count(distinct case when snapshot_date = signup_date_Est then subscriber_id else null end) as weekly_site_nsts
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b  --live user filter
    where 1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    and snapshot_Date between '2020-03-02' and (select * from last_sunday_Date)
    group by 1
)


  select
  aa.sunday_date,
  aa.network,
  aa.cla_status,
  avail_titles,
  avail_episodes,
  avail_hours,
  avail_titles / total_titles as pct_titles,
  avail_episodes / total_episodes as pct_episodes,
  avail_hours / total_hours as pct_hours,
  network_hours,
  network_actives,
  network_nsts,
  network_first_streams,
  network_hours / avail_hours as volume_efficiency,
  network_hours / weekly_site_hours as pct_network_hours,
  network_nsts / weekly_site_nsts as pct_network_nsts,
  network_first_streams / weekly_site_first_streams as pct_network_first_Streams,
  network_actives / entitled_subs as pct_network_reach,
  lag(volume_efficiency) over (partition by aa.network,aa.cla_status order by aa.sunday_date) as lag_volume_efficiency,
  lag(pct_network_hours) over (partition by aa.network,aa.cla_status order by aa.sunday_date) as lag_network_hours,
  lag(pct_network_nsts) over (partition by aa.network,aa.cla_status order by aa.sunday_date) as lag_network_nsts,
  lag(pct_network_first_Streams) over (partition by aa.network,aa.cla_status order by aa.sunday_date) as lag_network_first_streams,
  lag(pct_network_reach) over (partition by aa.network,aa.cla_status order by aa.sunday_date) as lag_network_reach
  from (
      select
      a.sunday_date,
      a.network,
      coalesce(a.content_type,'Total') as cla_status,
      network_hours,
      network_actives,
      network_nsts,
      network_first_streams,
      entitled_subs,
      weekly_site_nsts,
      sum(case when cla_status <> 'Total' then network_hours else 0 end) over (partition by a.sunday_date) as weekly_site_hours,
      sum(case when cla_status <> 'Total' then network_first_streams else 0 end) over (partition by a.sunday_date) as weekly_site_first_streams
      from (
          select
          last_day(to_date(a.calendar_date),'week') as sunday_date,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
              when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
              when v.channel in ('FX','FXX') then 'FX'
              when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
              when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
              when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
              else v.channel
          end as network,
          cl.content_type,
          sum(playback_watched_ms/3600000) as network_hours,
          count(distinct case when has_Watched_threshold = 1 then a.subscriber_id else null end) as network_actives,
          count(distinct case when has_Watched_threshold = 1 and days_since_signup = 0 then a.subscriber_id else null end) as network_nsts,
          count(distinct fs.subscriber_id) as network_first_streams
          from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
          left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs 
              on fs.calendar_Date = a.calendar_Date
                  and fs.subscriber_id = a.subscriber_id
                      and fs.video_id = a.video_id
                          and fs.is_first_stream_overall = 1 
          join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
              on a.video_id = v.video_id
                  and v.programming_type in ('Full Episode','Full Movie')
          left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
              on mtt.content_title = v.content_title
          join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
              on a.content_partner_id = c.content_partner_id
          join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
              on a.bundle_package_sk = p.bundle_package_sk 
                  and p.content_source_group = 'SVOD'
          left join content_license_grouping cl
              on cl.sunday_date = last_day(to_date(a.calendar_date),'week')
                  and cl.video_id = a.video_id
          where 1=1
          and a.calendar_date between '2020-03-02' and (select * from last_sunday_Date)
          group by 1,2,cube(3)
      ) a 
      join sub_base s 
          on s.sunday_date = a.sunday_date
      group by 1,2,3,4,5,6,7,8,9
  ) aa 
  join metadata m 
      on m.sunday_date = aa.sunday_date
      and m.network = aa.network
      and m.cla_status = aa.cla_status
  where 1=1
  and aa.network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

;








------------------------------------------




CREATE OR REPLACE TABLE "DEV"."PUBLIC"."NETWORK_SERIES_LEVEL_ENGAGEMENT" AS

with last_sunday_Date as (
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)
,

dates as 
( 
    select
    weekenddate as week
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where realdate between '2020-03-02' and (select * from last_sunday_Date)
    group by 1
)
,


video_id_license_status as
(
    select
    last_day(to_date(calendar_date),'week') as sunday_date,
    content_title,
    a.video_id,
    cla_current_status,
    content_partner_name,
    channel,
    license_type,
    film_window_type
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
    join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        on a.video_id = v.video_id
        and v.programming_type in ('Full Episode','Full Movie')
    left join "UNIVERSE360"."CONTENT"."DIM_VIDEO_FILM_WINDOW" vw 
        on vw.video_id = a.video_id
        and a.calendar_Date between film_window_start_date and film_window_end_Date
    join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
        on a.content_partner_id = c.content_partner_id
    join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
        on a.bundle_package_sk = p.bundle_package_sk 
        and p.content_source_group = 'SVOD'
    where 1=1
    and calendar_date between '2020-03-02' and (select * from last_sunday_Date)
    group by 1,2,3,4,5,6,7,8
)
,

content_license_grouping as (
    select
    *
    from(
        select
        v.sunday_date,
        v.content_title,
        v.video_id,
        case
            when film_window_type is not null then film_window_type
            when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and license_type <> 'original' and channel not in ('FX','FXX'))) then 'Current'
            when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 'Exclusives'
            when v.cla_current_status = 'current_prior' then 'Prior' 
            else 'Library'
        end as content_type,
        case
            when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and license_type <> 'original' and channel not in ('FX','FXX'))) then 1
            when v.cla_current_status = 'current_prior' then 2
            when film_window_type is not null then 3
            when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 4
            else 5
        end as content_type_rank,
        min(content_type_rank) over (partition by v.sunday_date, v.content_title, v.video_id) as rank_use
        from video_id_license_status v
        where 1 = 1
        group by 1,2,3,4,5
        order by 1,2
    )
    where rank_use = content_type_rank
)
,

metadata as (
    select
    week as sunday_date,
    network,
    coalesce(content_type, 'Total') as cla_status,
    content_title,
    max(episodes) as avail_episodes,
    max(hours) as avail_hours
    from
    (
        select
        week,
        network,
        content_title,
        content_type,
        count(distinct season_number || ' - ' || episode_number) as episodes,
        sum(hours) as hours
        from 
        (
            select
            d.week,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
            v.content_title,
            cl.content_type,
            v.season_number,
            v.episode_number,
            max(video_length)/3600000 as hours
            from UNIVERSE360.CONTENT.VAW_HISTORY_DAY_EST_LATEST a
            join UNIVERSE360.CONTENT.DIM_CONTENT_PARTNER c 
              on a.content_partner_id = c.content_partner_id
            join UNIVERSE360.CONTENT.DIM_VIDEO v 
              on a.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join dates d 
              on window_start_date <= d.week and window_end_date >= dateadd('day',-6,d.week)
            left join content_license_grouping cl 
              on cl.sunday_date = d.week
              and cl.video_id = a.video_id
            where 1=1     
            and package_name in ('NOAH SVOD','Plus')
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
            group by 1,2,3,4,5,6
        ) a
        group by 1,2,3,cube(4)
    )
    group by 1,2,3,4
)
,

sub_base as
( 
    select
    last_day(to_date(snapshot_date),'week') as sunday_date,
    count(distinct subscriber_id) as entitled_subs,
    count(distinct case when snapshot_date = signup_date_Est then subscriber_id else null end) as weekly_site_nsts
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b  --live user filter
    where 1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    and snapshot_Date between '2020-03-02' and (select * from last_sunday_Date)
    group by 1
)



select
sunday_date,
network,
programming_type,
genre,
content_title,
cla_status,
avail_episodes,
avail_hours,
content_hours,
content_actives,
content_nsts,
network_first_streams,
volume_efficiency,
pct_network_hours,
pct_network_nsts,
pct_network_first_Streams,
pct_network_reach,
lag_volume_efficiency,
lag_network_hours,
lag_network_nsts,
lag_network_first_Streams,
lag_network_reach,
rank_volume_efficiency,
rank_network_hours,
rank_network_nsts,
rank_network_first_Streams,
rank_network_reach,
rank_genre_volume_efficiency,
rank_genre_network_hours,
rank_genre_network_nsts,
rank_genre_network_first_Streams,
rank_genre_network_reach,
lag(rank_volume_efficiency) over (partition by content_title, network, cla_status order by sunday_date) as lag_rank_volume_efficiency,
lag(rank_network_hours) over (partition by content_title, network, cla_status order by sunday_date) as lag_rank_network_hours,
lag(rank_network_nsts) over (partition by content_title, network, cla_status order by sunday_date) as lag_rank_network_nsts,
lag(rank_network_first_Streams) over (partition by content_title, network, cla_status order by sunday_date) as lag_rank_network_first_streams,
lag(rank_network_reach) over (partition by content_title, network, cla_status order by sunday_date) as lag_rank_network_reach,
lag(rank_genre_volume_efficiency) over (partition by content_title, network, cla_status order by sunday_date) as lag_genre_rank_volume_efficiency,
lag(rank_genre_network_hours) over (partition by content_title, network, cla_status order by sunday_date) as lag_genre_rank_network_hours,
lag(rank_genre_network_nsts) over (partition by content_title, network, cla_status order by sunday_date) as lag_genre_rank_network_nsts,
lag(rank_genre_network_first_Streams) over (partition by content_title, network, cla_status order by sunday_date) as lag_genre_rank_network_first_Streams,
lag(rank_genre_network_reach) over (partition by content_title, network, cla_status order by sunday_date) as lag_genre_rank_network_reach
from (
    select
    aa.sunday_date,
    aa.network,
    programming_type,
    genre,
    aa.content_title,
    aa.cla_status,
    avail_episodes,
    avail_hours,
    content_hours,
    content_actives,
    content_nsts,
    network_first_streams,
    content_hours / avail_hours as volume_efficiency,
    content_hours / weekly_site_hours as pct_network_hours,
    content_nsts / weekly_site_nsts as pct_network_nsts,
    network_first_streams / weekly_site_first_streams as pct_network_first_Streams,
    content_actives / entitled_subs as pct_network_reach,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type order by volume_efficiency desc) as rank_volume_efficiency,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type order by pct_network_hours desc) as rank_network_hours,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type order by pct_network_nsts desc) as rank_network_nsts,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type order by pct_network_first_Streams desc) as rank_network_first_Streams,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type order by pct_network_reach desc) as rank_network_reach,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type, genre order by volume_efficiency desc) as rank_genre_volume_efficiency,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type, genre order by pct_network_hours desc) as rank_genre_network_hours,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type, genre order by pct_network_nsts desc) as rank_genre_network_nsts,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type, genre order by pct_network_first_Streams desc) as rank_genre_network_first_Streams,
    rank() over (partition by aa.sunday_date, aa.cla_status, programming_type, genre order by pct_network_reach desc) as rank_genre_network_reach,
    lag(volume_efficiency) over (partition by aa.content_title, aa.network, aa.cla_status order by aa.sunday_date) as lag_volume_efficiency,
    lag(pct_network_hours) over (partition by aa.content_title, aa.network, aa.cla_status order by aa.sunday_date) as lag_network_hours,
    lag(pct_network_nsts) over (partition by aa.content_title, aa.network, aa.cla_status order by aa.sunday_date) as lag_network_nsts,
    lag(pct_network_first_Streams) over (partition by aa.content_title, aa.network, aa.cla_status order by aa.sunday_date) as lag_network_first_Streams,
    lag(pct_network_reach) over (partition by aa.content_title, aa.network, aa.cla_status order by aa.sunday_date) as lag_network_reach
    from (
        select
        a.sunday_date,
        a.network,
        programming_type,
        genre,
        coalesce(a.content_type,'Total') as cla_status,
        content_title,
        content_hours,
        content_actives,
        content_nsts,
        network_first_streams,
        entitled_subs,
        weekly_site_nsts,
        sum(case when cla_status <> 'Total' then content_hours else 0 end) over (partition by a.sunday_date) as weekly_site_hours,
        sum(case when cla_status <> 'Total' then network_first_streams else 0 end) over (partition by a.sunday_date) as weekly_site_first_streams
        from (
            select
            last_day(to_date(a.calendar_date),'week') as sunday_date,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
            v.programming_type,
            v.series_budget_vertical as genre,
            v.content_title,
            cl.content_type,
            sum(playback_watched_ms/3600000) as content_hours,
            count(distinct case when has_Watched_threshold = 1 then a.subscriber_id else null end) as content_actives,
            count(distinct case when has_Watched_threshold = 1 and days_since_signup = 0 then a.subscriber_id else null end) as content_nsts,
            count(distinct fs.subscriber_id) as network_first_streams
            from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
            left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs 
                on fs.calendar_Date = a.calendar_Date
                  and fs.subscriber_id = a.subscriber_id
                    and fs.video_id = a.video_id
                      and fs.is_first_stream_overall = 1 
            join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
                on a.video_id = v.video_id
                and v.programming_type in ('Full Episode','Full Movie')
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
                on a.content_partner_id = c.content_partner_id
            join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
                on a.bundle_package_sk = p.bundle_package_sk 
                and p.content_source_group = 'SVOD'
            left join content_license_grouping cl
                on cl.sunday_date = last_day(to_date(a.calendar_date),'week')
                and cl.video_id = a.video_id
            where 1=1
            and a.calendar_date between '2020-03-02' and (select * from last_sunday_Date)
            group by 1,2,3,4,5,cube(6)
        ) a 
        join sub_base s 
            on s.sunday_date = a.sunday_date
        group by 1,2,3,4,5,6,7,8,9,10,11,12
    ) aa 
    join metadata m 
        on m.sunday_date = aa.sunday_date
        and m.network = aa.network
        and m.cla_status = aa.cla_status
        and m.content_title = aa.content_title
    where 1=1
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
where 1=1
and network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
order by 1,2,3,4;







-------------------------------------------------




CREATE OR REPLACE TABLE "DEV"."PUBLIC"."NETWORK_LEVEL_DEMO" AS
with last_sunday_Date as (
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)
,

demo_breakout as (
    select
    sunday_Date,
    network,
    gender,
    age,
    weekly_network_gender_actives / nullif(weekly_network_actives,0) as pct_gender,
    weekly_network_age_actives / nullif(weekly_network_actives,0) as pct_age
    from (
        select
        sunday_Date,
        coalesce(network_breakout,'Total') as network,
        gender_breakout as gender,
        age_breakout as age,
        actives,
        sum(actives) over (partition by sunday_Date, network) as weekly_network_actives,
        sum(actives) over (partition by sunday_Date, network, gender) as weekly_network_gender_actives,
        sum(actives) over (partition by sunday_Date, network, age) as weekly_network_age_actives
        from (
            select
            d.weekenddate as sunday_Date,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network_breakout,
            case 
                when u.age < 13 then '<13'
                when u.age < 18 then '13-17'
                when u.age < 25 then '18-24'
                when u.age < 35 then '25-34'
                when u.age < 45 then '35-44'
                when u.age < 55 then '45-54'
                when u.age >= 55 then '55+'
            end as Age_breakout,
            case 
                when u.gender = 'm' then 'Male' 
                when u.gender = 'f' then 'Female' 
                else 'Unknown' 
            end as Gender_breakout,
            count(distinct case when f.has_watched_threshold = 1 then f.subscriber_id else null end) as actives
            from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
            JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
                ON f.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p 
                on p.bundle_package_sk = f.bundle_package_sk
            join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c 
                on c.content_partner_id = f.content_partner_id
            join CONTENT_MART.DEFAULT.VW_REGISTERED_USER u 
                on u.userid = f.user_id
            join CONTENT_MART_SHARE.DEFAULT.EST_DAY d 
                on d.realdate = f.calendar_date
            where
            1=1
            and f.calendar_date between '2020-03-02' and (select * from last_sunday_Date)
            and v.programming_type in ('Full Episode','Full Movie')
            and p.content_source_group = 'SVOD'
            and u.age between 18 and 80
            and u.gender in ('m','f')
            group by 1,cube(2),3,4
        )
        group by 1,2,3,4,5
    )
    order by 1,2
)

select
d.sunday_Date,
d.network,
d.gender,
d.age,
d.pct_gender,
d.pct_age,
db.pct_gender as pct_site_gender,
db.pct_age as pct_site_gender_age
from demo_breakout d 
join demo_breakout db 
    on d.sunday_Date = db.sunday_Date
    and d.gender = db.gender
    and d.age = db.age
where 1=1
and db.network = 'Total'
and d.network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
order by 1,2;














---=------------------------------------------------


CREATE OR REPLACE TABLE "DEV"."PUBLIC"."NETWORK_TITLE_LEVEL_DEMO" AS
with last_sunday_Date as (
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)



    select
    sunday_Date,
    network,
    content_title,
    gender,
    age,
    weekly_content_gender_actives / nullif(weekly_content_actives,0) as pct_gender,
    weekly_content_age_actives / nullif(weekly_content_actives,0) as pct_age
    from (
        select
        sunday_Date,
        coalesce(network_breakout,'Total') as network,
        content_title,
        gender_breakout as gender,
        age_breakout as age,
        actives,
        sum(actives) over (partition by sunday_Date, content_title) as weekly_content_actives,
        sum(actives) over (partition by sunday_Date, content_title, gender) as weekly_content_gender_actives,
        sum(actives) over (partition by sunday_Date, content_title, age) as weekly_content_age_actives
        from (
            select
            d.weekenddate as sunday_Date,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network_breakout,
            content_title,
            case 
                when u.age < 13 then '<13'
                when u.age < 18 then '13-17'
                when u.age < 25 then '18-24'
                when u.age < 35 then '25-34'
                when u.age < 45 then '35-44'
                when u.age < 55 then '45-54'
                when u.age >= 55 then '55+'
            end as Age_breakout,
            case 
                when u.gender = 'm' then 'Male' 
                when u.gender = 'f' then 'Female' 
                else 'Unknown' 
            end as Gender_breakout,
            count(distinct case when f.has_watched_threshold = 1 then f.subscriber_id else null end) as actives
            from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
            JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
                ON f.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p 
                on p.bundle_package_sk = f.bundle_package_sk
            join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c 
                on c.content_partner_id = f.content_partner_id
            join CONTENT_MART.DEFAULT.VW_REGISTERED_USER u 
                on u.userid = f.user_id
            join CONTENT_MART_SHARE.DEFAULT.EST_DAY d 
                on d.realdate = f.calendar_date
            where
            1=1
            and f.calendar_date between '2020-03-02' and (select * from last_sunday_Date)
            and v.programming_type in ('Full Episode','Full Movie')
            and p.content_source_group = 'SVOD'
            and u.age between 18 and 80
            and u.gender in ('m','f')
            group by 1,2,3,4,5
        )
        group by 1,2,3,4,5,6
    )
    where
    1=1
    and network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
    order by 1,2

;













create or replace table "DEV"."PUBLIC"."NETWORK_FY_TITLE_PERFORMANCE" as
with last_sunday_Date as ( --get latest sunday
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)
,

l_52 as (
    select last_sunday
        ,  to_date(dateadd('week',-52,last_sunday)) as first_sunday
    from last_sunday_date
)
,

metadata as (
    select
    1 as fake_join,
    network,
    content_title,
    programming_type,
    max(episodes) as avail_episodes,
    max(hours) as avail_hours
    from
    (
        select
        network,
        content_title,
        programming_type,
        count(distinct season_number || ' - ' || episode_number) as episodes,
        sum(hours) as hours
        from 
        (
            select
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
            v.content_title,
            v.programming_type,
            v.season_number,
            v.episode_number,
            max(video_length)/3600000 as hours
            from UNIVERSE360.CONTENT.VAW_HISTORY_DAY_EST_LATEST a
            join UNIVERSE360.CONTENT.DIM_CONTENT_PARTNER c 
              on a.content_partner_id = c.content_partner_id
            join UNIVERSE360.CONTENT.DIM_VIDEO v 
              on a.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join l_52 l 
              on window_start_date <= last_sunday and window_end_date >= first_sunday
            where 1=1     
            and package_name in ('NOAH SVOD','Plus')
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
            group by 1,2,3,4,5
        ) a
        group by 1,2,3
    )
    group by 1,2,3,4
)
,

sub_base as
( 
    select
    1 as fake_join,
    count(distinct subscriber_id) as entitled_subs,
    count(distinct case when snapshot_date = signup_date_Est then subscriber_id else null end) as fy_nsts
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b 
    join l_52 l
        on b.snapshot_date between l.first_sunday and l.last_sunday
    where 1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    group by 1
)
,

hours_base as (
    select
    1 as fake_join,
    sum(playback_watched_ms/3600000) as fy_hours
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
    join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        on a.video_id = v.video_id
        and v.programming_type in ('Full Episode','Full Movie')
    join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
        on a.content_partner_id = c.content_partner_id
    join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
        on a.bundle_package_sk = p.bundle_package_sk 
        and p.content_source_group = 'SVOD'
    join l_52 l
        on calendar_date between l.first_sunday and l.last_sunday
    where 1=1
    group by 1
)

select *
from (
    select
    a.network,
    a.content_title,
    series_budget_vertical,
    a.programming_type,
    network_hours,
    network_hours / fy_hours as pct_hours,
    network_actives,
    network_actives / entitled_subs as pct_reach,
    network_nsts,
    network_nsts / fy_nsts as pct_nsts,
    network_first_streams,
    network_first_streams / fy_first_streams as pct_first_Streams,
    network_hours / avail_hours as volume_efficiency,
    rank() over (partition by  a.programming_type order by network_hours desc) as hours_rank,
    rank() over (partition by  a.programming_type order by network_actives desc) as actives_rank,
    rank() over (partition by  a.programming_type order by network_nsts desc) as nsts_rank,
    rank() over (partition by  a.programming_type order by network_first_streams desc) as FIRST_STREAMS_rank,
    rank() over (partition by  a.programming_type order by volume_efficiency desc) as volume_efficiency_rank,
    rank() over (partition by  a.programming_type, series_budget_vertical order by network_hours desc) as genre_hours_rank,
    rank() over (partition by  a.programming_type, series_budget_vertical order by network_actives desc) as genre_actives_rank,
    rank() over (partition by  a.programming_type, series_budget_vertical order by network_nsts desc) as genre_nsts_rank,
    rank() over (partition by  a.programming_type, series_budget_vertical order by network_first_streams desc) as genre_FIRST_STREAMS_rank,
    rank() over (partition by  a.programming_type, series_budget_vertical order by volume_efficiency desc) as genre_volume_efficiency_rank
    from (
        select
        1 as fake_join,
        v.content_title,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
        end as network,
        programming_type,
        v.series_budget_vertical,
        sum(playback_watched_ms/3600000) as network_hours,
        count(distinct case when has_Watched_threshold = 1 then a.subscriber_id else null end) as network_actives,
        count(distinct case when has_Watched_threshold = 1 and days_since_signup = 0 then a.subscriber_id else null end) as network_nsts,
        count(distinct fs.subscriber_id) as network_first_streams,
        sum(network_first_streams) over () as fy_first_streams
        from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
        left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs 
            on fs.calendar_Date = a.calendar_Date
                and fs.subscriber_id = a.subscriber_id
                    and fs.video_id = a.video_id
                        and fs.is_first_stream_overall = 1 
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
        left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
            on mtt.content_title = v.content_title
        join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
            on a.content_partner_id = c.content_partner_id
        join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
            on a.bundle_package_sk = p.bundle_package_sk 
            and p.content_source_group = 'SVOD'
        join l_52 l 
            on A.calendar_date between l.first_sunday and l.last_sunday
        where 1=1
        group by 1,2,3,4,5
    ) a
    join metadata m 
        on a.content_title = m.content_title
        and a.programming_type = m.programming_type
        and a.fake_join = m.fake_join
        and a.network = m.network
    join sub_base s 
        on s.fake_join = a.fake_join
    join hours_base h 
        on h.fake_join = a.fake_join
)
where 1=1
and network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
order by 1,2,3 desc
;












CREATE OR REPLACE TABLE DEV.PUBLIC.NETWORK_FY_PERFORMANCE as

with last_sunday_Date as ( --get latest sunday
    select
    '2021-08-15'  as last_sunday
)
,

l_52 as (
    select last_sunday
        ,  to_date(dateadd('week',-52,last_sunday)) as first_sunday
    from last_sunday_date
)
,

metadata as (
    select
    1 as fake_join,
    network,
    max(episodes) as avail_episodes,
    max(hours) as avail_hours
    from
    (
        select
        network,
        count(distinct content_title || ' - ' || season_number || ' - ' || episode_number) as episodes,
        sum(hours) as hours
        from 
        (
            select
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
            v.content_title,
            v.programming_type,
            v.season_number,
            v.episode_number,
            max(video_length)/3600000 as hours
            from UNIVERSE360.CONTENT.VAW_HISTORY_DAY_EST_LATEST a
            join UNIVERSE360.CONTENT.DIM_CONTENT_PARTNER c 
              on a.content_partner_id = c.content_partner_id
            join UNIVERSE360.CONTENT.DIM_VIDEO v 
              on a.video_id = v.video_id
            left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
                on mtt.content_title = v.content_title
            join l_52 l 
              on window_start_date <= last_sunday and window_end_date >= first_sunday
            where 1=1     
            and package_name in ('NOAH SVOD','Plus')
            and v.programming_type in ('Full Episode','Full Movie')
            and v.asset_playback_type = 'VOD'
            group by 1,2,3,4,5
        ) a
        group by 1
    )
    group by 1,2
)
,

sub_base as
( 
    select
    1 as fake_join,
    count(distinct subscriber_id) as entitled_subs,
    count(distinct case when snapshot_date = signup_date_Est then subscriber_id else null end) as fy_nsts
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b 
    join l_52 l
        on b.snapshot_date between l.first_sunday and l.last_sunday
    where 1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    group by 1
)
,

hours_base as (
    select
    1 as fake_join,
    sum(playback_watched_ms/3600000) as fy_hours
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
    join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        on a.video_id = v.video_id
        and v.programming_type in ('Full Episode','Full Movie')
    join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
        on a.content_partner_id = c.content_partner_id
    join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
        on a.bundle_package_sk = p.bundle_package_sk 
        and p.content_source_group = 'SVOD'
    join l_52 l
        on calendar_date between l.first_sunday and l.last_sunday
    where 1=1
    group by 1
)

    select
    a.network,
    network_hours,
    network_hours / fy_hours as pct_hours,
    network_actives,
    network_actives / entitled_subs as pct_reach,
    network_nsts,
    network_nsts / fy_nsts as pct_nsts,
    network_first_streams,
    network_first_streams / fy_first_streams as pct_first_streams,
    network_hours / avail_hours as volume_efficiency
    from (
        select
        1 as fake_join,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
            end as network,
        sum(playback_watched_ms/3600000) as network_hours,
        count(distinct case when has_Watched_threshold = 1 then a.subscriber_id else null end) as network_actives,
        count(distinct case when has_Watched_threshold = 1 and days_since_signup = 0 then a.subscriber_id else null end) as network_nsts,
        count(distinct fs.subscriber_id) as network_first_streams,
        sum(network_first_streams) over () as fy_first_streams
        from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
        left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs 
            on fs.calendar_Date = a.calendar_Date
                and fs.subscriber_id = a.subscriber_id
                    and fs.video_id = a.video_id
                        and fs.is_first_stream_overall = 1 
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
        left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
            on mtt.content_title = v.content_title
        join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
            on a.content_partner_id = c.content_partner_id
        join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
            on a.bundle_package_sk = p.bundle_package_sk 
            and p.content_source_group = 'SVOD'
        join l_52 l 
            on a.calendar_date between l.first_sunday and l.last_sunday
        where 1=1
        group by 1,2
    ) a
        join metadata m 
            on a.fake_join = m.fake_join
            and a.network = m.network
        join sub_base s 
            on s.fake_join = a.fake_join
        join hours_base h 
            on h.fake_join = a.fake_join
    where 1=1
    and a.network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
    order by 1,2,3 desc





;















CREATE OR REPLACE TABLE "DEV"."PUBLIC"."ETHNICITY_NETWORK_INDEX" AS

with last_sunday_Date as (
    select
    max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where weekenddate <= current_Date
)
,


epsilon_subs as
( 
  select
  case 
    when segment_id in (888974152, 821865288, 922528584, 872196936) then  'White'
    when segment_id in (813476680) then 'Black / African American'
    when segment_id in (880585544) then 'Hispanic'
    when segment_id in (838642504, 830253896, 847031112, 930917192, 939305800) then 'Asian'
    else 'Other'
    end as ethnicity,
    userid as user_id
    from "DEV"."PUBLIC"."EPSILON_RAW_LATEST_TABLE" ep
    join "DEV"."PUBLIC"."EPSILON_TAXONOMY" t on t.segment_id = ep.segmentid and lower(t.path) like '%ethnicity%'
    group by 1,2
)
,

sub_base as
( 
    select
    last_day(to_date(snapshot_date),'week') as sunday_date,
    ethnicity,
    count(distinct subscriber_id) as entitled_subs
    from "UNIVERSE360"."SUBSCRIPTIONS"."USER_SUBSCRIPTION_SNAPSHOT" b  --live user filter
    join epsilon_subs e on e.user_id = b.userid
    where 1=1
    and b.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')
    and b.promotion_status_group IN ('PAID', 'PROMOTION')
    and b.activity_status_sk != 10
    and upper(b.base_program_type) != 'TEST'
    and b.user_deduped = 1
    AND snapshot_date BETWEEN '2020-03-02' and (select * from last_sunday_Date)
    group by 1, 2
)

select
a.sunday_Date,
network,
coalesce(content_title, 'Total Network') as content_title,
ethnicity_rollup,
a.ethnicity,
actives,
hours,
entitled_subs,
active_rank
from (
    select
    d.weekenddate as sunday_Date,
    case when ethnicity = 'White' then ethnicity else 'Non-White' end as ethnicity_rollup,
    ethnicity,
            case
                when mtt.dge_partner is not null then mtt.dge_partner
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when v.channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight'
                when v.channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when v.channel in ('FX','FXX') then 'FX'
                when v.channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when v.channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or v.channel = 'Hulu Original Series') and v.channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else v.channel
    end as network,
    content_title,
    count(distinct case when f.has_watched_threshold = 1 then f.subscriber_id else null end) as actives,
    sum(playback_Watched_ms / 3600000) as hours,
    rank() over (partition by ethnicity, sunday_Date, network order by actives desc) as active_rank
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" f
    JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
        ON f.video_id = v.video_id
    left join "DEV"."PUBLIC"."MANUAL_TITLE_TAG" mtt 
        on mtt.content_title = v.content_title
    join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p 
        on p.bundle_package_sk = f.bundle_package_sk
    join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c 
        on c.content_partner_id = f.content_partner_id
    join epsilon_subs e  
        on e.user_id = f.user_id
    join CONTENT_MART_SHARE.DEFAULT.EST_DAY d 
        on d.realdate = f.calendar_date
    where
    1=1
    and v.programming_type in ('Full Episode','Full Movie')
    and p.content_source_group = 'SVOD'
    AND calendar_date BETWEEN '2020-03-02' and (select * from last_sunday_Date)
    group by 1,2,3,4,cube(5)
) a
join sub_base s on s.sunday_Date = a.sunday_date and s.ethnicity = a.ethnicity
where 1=1
and network in ('Hotstar', 'Disney Branded TV', 'FOX', 'Searchlight', '20th Century', 'ABC OTV', 'FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'Hulu Originals')
;