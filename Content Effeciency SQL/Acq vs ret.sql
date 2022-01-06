CREATE OR REPLACE TABLE "DEV"."PUBLIC"."RETENTION_ACQUISITION_ENGAGEMENT_MONTHLY" AS
WITH engagement AS (
    SELECT date_trunc('month', pb.calendar_Date) as month
        ,  pb.subscriber_id     
        ,  sub.subscription_id
        ,  case
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight Pictures'
                when channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century Studios'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when channel in ('FX','FXX') then 'FX'
                when channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or channel = 'Hulu Original Series') and channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else channel
           end as network
        ,  vid.programming_type
        ,  vid.series_budget_Vertical
        ,  vid.seg_level_a_name
        ,  vid.seg_level_b_name
        ,  license_type
        ,  vid.content_title  
        ,  max(case when fs.subscriber_id is not null then 1 else 0 end) as first_Stream_flag
        ,  SUM(pb.playback_watched_ms)/3600000.0 AS total_watched_hr_series_per_sub 
    FROM UNIVERSE360.CONTENT.FACT_USER_CONTENT_CONSUMPTION_DAY AS pb
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" AS vid
                ON vid.video_id = pb.video_id
                    AND UPPER(vid.programming_type) IN ('FULL EPISODE', 'FULL MOVIE')
                        AND vid.asset_playback_type = 'VOD'
                            AND vid.series_id <> 'N/A'
                                AND vid.season_id <> 'N/A'
            INNER JOIN UNIVERSE360.CONTENT.DIM_BUNDLE_PACKAGE AS bdl
                ON bdl.bundle_package_sk = pb.bundle_package_sk
                    AND bdl.content_source_group = 'SVOD'
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" AS cp 
                ON cp.content_partner_id = pb.content_partner_id
            INNER JOIN UNIVERSE360.SUBSCRIPTIONS.USER_SUBSCRIPTION_SNAPSHOT AS sub
                ON sub.snapshot_date = pb.calendar_date
                    AND sub.userid = pb.user_id
                        AND sub.user_deduped = 1
                            and sub.base_product_group IN ('SVOD')
                                and sub.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')    -- users who still have access to the platform
                                    and sub.promotion_status_group IN ('PAID','PROMOTION')                                -- PAID or PROMO ONLY
                                        and sub.activity_status_sk != 10                                                      -- wholesale inactive
                                            and sub.base_program_type != 'Test'
            left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
                on fs.calendar_Date = pb.calendar_Date
                    and fs.subscriber_id = pb.subscriber_id
                        and fs.video_id = pb.video_id
                            and fs.is_first_stream_overall = 1
    WHERE 
        1=1
        and pb.calendar_date between '2021-01-01' and '2021-08-31'
        AND pb.has_watched_threshold = 'TRUE'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,

allocation AS ( 
    SELECT *
        ,  SUM(total_watched_hr_series_per_sub ) OVER (PARTITION BY month, subscription_id) AS total_watched_hr_per_sub 
        ,  total_watched_hr_series_per_sub  /total_watched_hr_per_sub AS series_engagement_share_per_sub
    FROM engagement
)
 
select * 
from(
    SELECT month
        ,  network
        ,  series_budget_Vertical
        ,  seg_level_a_name
        ,  seg_level_b_name
        ,  programming_type
        ,  license_type
        ,  content_title
        ,  sum(first_Stream_flag) as first_Streams
        ,  SUM(series_engagement_share_per_sub) AS attributable_subs
        ,  sum(total_watched_hr_series_per_sub) as hours
        ,  attributable_subs / sum(attributable_subs) over(partition by month) as pct_attr_subs
        ,  first_Streams / sum(first_Streams) over(partition by month) as pct_first_Streams
        ,  hours / sum(hours) over(partition by month) as pct_hours
        ,  pct_attr_subs / pct_hours as weighted_retention
        ,  pct_first_Streams / pct_hours as weighted_acquisition
    FROM allocation
    where
        1=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
where 
    1 = 1
    --and content_title = 'Nine Perfect Strangers'
;







CREATE OR REPLACE TABLE "DEV"."PUBLIC"."RETENTION_ACQUISITION_ENGAGEMENT_SEASON_MONTHLY" AS
WITH engagement AS (
    SELECT date_trunc('month', pb.calendar_Date) as month
        ,  pb.subscriber_id     
        ,  sub.subscription_id
        ,  case
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight Pictures'
                when channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century Studios'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when channel in ('FX','FXX') then 'FX'
                when channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or channel = 'Hulu Original Series') and channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else channel
           end as network
        ,  vid.programming_type
        ,  vid.series_budget_Vertical
        ,  vid.seg_level_a_name
        ,  vid.seg_level_b_name
        ,  license_type
        ,  vid.content_title  
        ,  vid.season_number
        ,  max(case when fs.subscriber_id is not null then 1 else 0 end) as first_Stream_flag
        ,  SUM(pb.playback_watched_ms)/3600000.0 AS total_watched_hr_series_per_sub 
    FROM UNIVERSE360.CONTENT.FACT_USER_CONTENT_CONSUMPTION_DAY AS pb
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" AS vid
                ON vid.video_id = pb.video_id
                    AND UPPER(vid.programming_type) IN ('FULL EPISODE', 'FULL MOVIE')
                        AND vid.asset_playback_type = 'VOD'
                            AND vid.series_id <> 'N/A'
                                AND vid.season_id <> 'N/A'
            INNER JOIN UNIVERSE360.CONTENT.DIM_BUNDLE_PACKAGE AS bdl
                ON bdl.bundle_package_sk = pb.bundle_package_sk
                    AND bdl.content_source_group = 'SVOD'
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" AS cp 
                ON cp.content_partner_id = pb.content_partner_id
            INNER JOIN UNIVERSE360.SUBSCRIPTIONS.USER_SUBSCRIPTION_SNAPSHOT AS sub
                ON sub.snapshot_date = pb.calendar_date
                    AND sub.userid = pb.user_id
                        AND sub.user_deduped = 1
                            and sub.base_product_group IN ('SVOD')
                                and sub.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')    -- users who still have access to the platform
                                    and sub.promotion_status_group IN ('PAID','PROMOTION')                                -- PAID or PROMO ONLY
                                        and sub.activity_status_sk != 10                                                      -- wholesale inactive
                                            and sub.base_program_type != 'Test'
            left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
                on fs.calendar_Date = pb.calendar_Date
                    and fs.subscriber_id = pb.subscriber_id
                        and fs.video_id = pb.video_id
                            and fs.is_first_stream_overall = 1
    WHERE 
        1=1
        and pb.calendar_date between '2021-01-01' and '2021-08-31'
        AND pb.has_watched_threshold = 'TRUE'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
,

allocation AS ( 
    SELECT *
        ,  SUM(total_watched_hr_series_per_sub ) OVER (PARTITION BY month, subscription_id) AS total_watched_hr_per_sub 
        ,  total_watched_hr_series_per_sub  /total_watched_hr_per_sub AS series_engagement_share_per_sub
    FROM engagement
)
 
select * 
from(
    SELECT month
        ,  network
        ,  series_budget_Vertical
        ,  seg_level_a_name
        ,  seg_level_b_name
        ,  programming_type
        ,  license_type
        ,  content_title
        ,  season_number
        ,  case when season_number <> 'N/A' then content_title || ' S' || season_number else content_title end as content_season
        ,  sum(first_Stream_flag) as first_Streams
        ,  SUM(series_engagement_share_per_sub) AS attributable_subs
        ,  sum(total_watched_hr_series_per_sub) as hours
        ,  attributable_subs / sum(attributable_subs) over(partition by month) as pct_attr_subs
        ,  first_Streams / sum(first_Streams) over(partition by month) as pct_first_Streams
        ,  hours / sum(hours) over(partition by month) as pct_hours
        ,  pct_attr_subs / pct_hours as weighted_retention
        ,  pct_first_Streams / pct_hours as weighted_acquisition
    FROM allocation
    where
        1=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
where 
    1 = 1
    --and content_title = 'Nine Perfect Strangers'
    
    
    
;











--Monthly  Series Level Harmony

current_task_realdate={{{ task_time_utc_realdate }}}
start_realdate=$(date -d "$current_task_realdate -1 day" +%Y-%m-01)   #get first of the month
end_realdate=$(date -d "$current_task_realdate -1 day" +%Y-%m-%d)  #get last day of the month


echo "Current Task Realdate: $current_task_realdate"
echo "Start Realdate: $start_realdate, End Realdate: $end_realdate"


table_name="DEV.PUBLIC.RETENTION_ACQUISITION_ENGAGEMENT_MONTHLY"


echo "Deleting rows within current month"
query="delete from ${table_name} where month = '${start_realdate}'"
echo "$query"

{{{snowsql_home}}}/snowsql -c hulux -r di_content_basic -w di_content_power -q "$query"


query="INSERT INTO ${table_name}
WITH engagement AS (
    SELECT date_trunc('month', pb.calendar_Date) as month
        ,  pb.subscriber_id     
        ,  sub.subscription_id
        ,  case
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight Pictures'
                when channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century Studios'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when channel in ('FX','FXX') then 'FX'
                when channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or channel = 'Hulu Original Series') and channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else channel
           end as network
        ,  vid.programming_type
        ,  vid.series_budget_Vertical
        ,  vid.seg_level_a_name
        ,  vid.seg_level_b_name
        ,  license_type
        ,  vid.content_title  
        ,  max(case when fs.subscriber_id is not null then 1 else 0 end) as first_Stream_flag
        ,  SUM(pb.playback_watched_ms)/3600000.0 AS total_watched_hr_series_per_sub 
    FROM UNIVERSE360.CONTENT.FACT_USER_CONTENT_CONSUMPTION_DAY AS pb
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" AS vid
                ON vid.video_id = pb.video_id
                    AND UPPER(vid.programming_type) IN ('FULL EPISODE', 'FULL MOVIE')
                        AND vid.asset_playback_type = 'VOD'
                            AND vid.series_id <> 'N/A'
                                AND vid.season_id <> 'N/A'
            INNER JOIN UNIVERSE360.CONTENT.DIM_BUNDLE_PACKAGE AS bdl
                ON bdl.bundle_package_sk = pb.bundle_package_sk
                    AND bdl.content_source_group = 'SVOD'
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" AS cp 
                ON cp.content_partner_id = pb.content_partner_id
            INNER JOIN UNIVERSE360.SUBSCRIPTIONS.USER_SUBSCRIPTION_SNAPSHOT AS sub
                ON sub.snapshot_date = pb.calendar_date
                    AND sub.userid = pb.user_id
                        AND sub.user_deduped = 1
                            and sub.base_product_group IN ('SVOD')
                                and sub.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')    -- users who still have access to the platform
                                    and sub.promotion_status_group IN ('PAID','PROMOTION')                                -- PAID or PROMO ONLY
                                        and sub.activity_status_sk != 10                                                      -- wholesale inactive
                                            and sub.base_program_type != 'Test'
            left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
                on fs.calendar_Date = pb.calendar_Date
                    and fs.subscriber_id = pb.subscriber_id
                        and fs.video_id = pb.video_id
                            and fs.is_first_stream_overall = 1
    WHERE 
        1=1
        and pb.calendar_date between '${start_realdate}' and '${end_realdate}'
        AND pb.has_watched_threshold = 'TRUE'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,

allocation AS ( 
    SELECT *
        ,  SUM(total_watched_hr_series_per_sub ) OVER (PARTITION BY month, subscription_id) AS total_watched_hr_per_sub 
        ,  total_watched_hr_series_per_sub  /total_watched_hr_per_sub AS series_engagement_share_per_sub
    FROM engagement
)
 
select * 
from(
    SELECT month
        ,  network
        ,  series_budget_Vertical
        ,  seg_level_a_name
        ,  seg_level_b_name
        ,  programming_type
        ,  license_type
        ,  content_title
        ,  sum(first_Stream_flag) as first_Streams
        ,  SUM(series_engagement_share_per_sub) AS attributable_subs
        ,  sum(total_watched_hr_series_per_sub) as hours
        ,  attributable_subs / sum(attributable_subs) over(partition by month) as pct_attr_subs
        ,  first_Streams / sum(first_Streams) over(partition by month) as pct_first_Streams
        ,  hours / sum(hours) over(partition by month) as pct_hours
        ,  pct_attr_subs / pct_hours as weighted_retention
        ,  pct_first_Streams / pct_hours as weighted_acquisition
    FROM allocation
    where
        1=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
where 
    1 = 1
"

echo "$query"

{{{snowsql_home}}}/snowsql -c hulux -r di_content_basic -w di_content_power -q "$query"











--Monthly Season Level Harmony

current_task_realdate={{{ task_time_utc_realdate }}}
start_realdate=$(date -d "$current_task_realdate -1 day" +%Y-%m-01)   #get first of the month
end_realdate=$(date -d "$current_task_realdate -1 day" +%Y-%m-%d)  #get last day of the month


echo "Current Task Realdate: $current_task_realdate"
echo "Start Realdate: $start_realdate, End Realdate: $end_realdate"


table_name="DEV.PUBLIC.RETENTION_ACQUISITION_ENGAGEMENT_SEASON_MONTHLY"


echo "Deleting rows within current month"
query="delete from ${table_name} where month = '${start_realdate}'"
echo "$query"

{{{snowsql_home}}}/snowsql -c hulux -r di_content_basic -w di_content_power -q "$query"


query="INSERT INTO ${table_name}
WITH engagement AS (
    SELECT date_trunc('month', pb.calendar_Date) as month
        ,  pb.subscriber_id     
        ,  sub.subscription_id
        ,  case
                when parent_content_partner_name = 'Hotstar' then 'Hotstar'
                when parent_content_partner_name = 'Disney' and channel in ('Disney Channel', 'Disney Junior', 'Disney XD') then 'Disney Branded TV'
                when parent_content_partner_name = 'Disney' and channel in ('FOX') then 'FOX'
                when channel = 'Fox Searchlight' and parent_content_partner_name = 'Disney' then 'Searchlight Pictures'
                when channel = 'Twentieth Century Fox' and parent_content_partner_name = 'Disney' then '20th Century Studios'
                when content_partner_name = 'NS CP ABC OTV Licensed' and parent_content_partner_name = 'Disney' then 'ABC OTV'
                when channel in ('FX','FXX') then 'FX'
                when channel = 'Freeform' and parent_content_partner_name = 'Disney' then 'Freeform'
                when channel = 'Freeform' and parent_content_partner_name <> 'Disney' then 'Freeform-Licensed'
                when ((license_type = 'original' or channel = 'Hulu Original Series') and channel not in ('FX', 'ABC News', 'ABC', 'Freeform', 'National Geographic', 'FXX')) then 'Hulu Originals'
                else channel
           end as network
        ,  vid.programming_type
        ,  vid.series_budget_Vertical
        ,  vid.seg_level_a_name
        ,  vid.seg_level_b_name
        ,  license_type
        ,  vid.content_title  
        ,  vid.season_number
        ,  max(case when fs.subscriber_id is not null then 1 else 0 end) as first_Stream_flag
        ,  SUM(pb.playback_watched_ms)/3600000.0 AS total_watched_hr_series_per_sub 
    FROM UNIVERSE360.CONTENT.FACT_USER_CONTENT_CONSUMPTION_DAY AS pb
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_VIDEO" AS vid
                ON vid.video_id = pb.video_id
                    AND UPPER(vid.programming_type) IN ('FULL EPISODE', 'FULL MOVIE')
                        AND vid.asset_playback_type = 'VOD'
                            AND vid.series_id <> 'N/A'
                                AND vid.season_id <> 'N/A'
            INNER JOIN UNIVERSE360.CONTENT.DIM_BUNDLE_PACKAGE AS bdl
                ON bdl.bundle_package_sk = pb.bundle_package_sk
                    AND bdl.content_source_group = 'SVOD'
            INNER JOIN "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" AS cp 
                ON cp.content_partner_id = pb.content_partner_id
            INNER JOIN UNIVERSE360.SUBSCRIPTIONS.USER_SUBSCRIPTION_SNAPSHOT AS sub
                ON sub.snapshot_date = pb.calendar_date
                    AND sub.userid = pb.user_id
                        AND sub.user_deduped = 1
                            and sub.base_product_group IN ('SVOD')
                                and sub.activity_status_name IN ('GOOD STANDING', 'PENDING CANCEL','GRACE PERIOD')    -- users who still have access to the platform
                                    and sub.promotion_status_group IN ('PAID','PROMOTION')                                -- PAID or PROMO ONLY
                                        and sub.activity_status_sk != 10                                                      -- wholesale inactive
                                            and sub.base_program_type != 'Test'
            left join "UNIVERSE360"."CONTENT"."FACT_USER_VOD_FIRST_STREAM_DAY" fs
                on fs.calendar_Date = pb.calendar_Date
                    and fs.subscriber_id = pb.subscriber_id
                        and fs.video_id = pb.video_id
                            and fs.is_first_stream_overall = 1
    WHERE 
        1=1
        and pb.calendar_date between '${start_realdate}' and '${end_realdate}'
        AND pb.has_watched_threshold = 'TRUE'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
,

allocation AS ( 
    SELECT *
        ,  SUM(total_watched_hr_series_per_sub ) OVER (PARTITION BY month, subscription_id) AS total_watched_hr_per_sub 
        ,  total_watched_hr_series_per_sub  /total_watched_hr_per_sub AS series_engagement_share_per_sub
    FROM engagement
)
 
select * 
from(
    SELECT month
        ,  network
        ,  series_budget_Vertical
        ,  seg_level_a_name
        ,  seg_level_b_name
        ,  programming_type
        ,  license_type
        ,  content_title
        ,  season_number
        ,  case when season_number <> 'N/A' then content_title || ' S' || season_number else content_title end as content_season
        ,  sum(first_Stream_flag) as first_Streams
        ,  SUM(series_engagement_share_per_sub) AS attributable_subs
        ,  sum(total_watched_hr_series_per_sub) as hours
        ,  attributable_subs / sum(attributable_subs) over(partition by month) as pct_attr_subs
        ,  first_Streams / sum(first_Streams) over(partition by month) as pct_first_Streams
        ,  hours / sum(hours) over(partition by month) as pct_hours
        ,  pct_attr_subs / pct_hours as weighted_retention
        ,  pct_first_Streams / pct_hours as weighted_acquisition
    FROM allocation
    where
        1=1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
where 
    1 = 1
"

echo "$query"

{{{snowsql_home}}}/snowsql -c hulux -r di_content_basic -w di_content_power -q "$query"






