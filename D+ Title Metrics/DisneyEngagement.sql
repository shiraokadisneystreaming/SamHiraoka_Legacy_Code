
CREATE OR REPLACE TABLE "DSS_DEV"."DISNEY_PLUS"."SH_MD_WINDOW_DBT" AS
with metadata as (
    select coalesce(m.partnerseriesid, m.programid) as content_unit_id
        ,  case
                when m.content_class = 'series' then 'Series'
                when m.content_class = 'movie' then 'Movie' 
                else 'Short-Form'
           end as content_type
        ,  studio_level_two as studio
        ,  max(case when m.is_disney_plus_original = 1 then 'Original' else 'Library' end) as window_type
        ,  max(ss.segment_level_a_name) as content_segment
        ,  max(coalesce(m.series_full_title,m.program_full_title)) as content_title
    from DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA as m
        join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_STUDIO_GROUPINGS_v sg
            on sg.content_unit_id = coalesce(m.partnerseriesid, m.programid)
        join DSS_PROD.DPLUS_ANALYTICS.DIM_DISNEY_CONTENT_SEGMENTS as ss
            on ss.CONTENT_UNIT_ID = coalesce(m.partnerseriesid, m.programid)
    where 
        1=1
        and CONTENTTYPE in ('full')
    group by 1, 2, 3
)
,
subs as (
    select coalesce(demo_segment, 'All Subs') as demo 
        ,  total_entitled_accounts
    from(
        select case
                    when d.account_id is null then 'Unknown'
                    when d.demo_segment_with_gender in ('4) households w/o children and male account holder', '3) households w/o children and female account holder') 
                        and account_holder_age_predict in ('18-24', '25-34') then account_holder_gender_predict || ', 18-34'
                    when d.demo_segment_with_gender in ('4) households w/o children and male account holder', '3) households w/o children and female account holder') 
                        and account_holder_age_predict in ('35-44', '45-54', '55-64', '65+') then account_holder_gender_predict || ', 35+'
                    when d.demo_segment_with_gender = '2) households with older children' then 'HH w/ Older Children'
                    when d.demo_segment_with_gender = '1) households with young children' then 'HH w/ Young Children'
            end as demo_segment
            ,  count(distinct s.account_id) as total_entitled_accounts
        from "DSS_PROD"."DISNEY_PLUS"."DIM_DISNEY_DAILY_ACCOUNT_ENGAGEMENT" as s
        join dss_prod.disney_plus.disney_demo_fully_assigned as d
            on s.account_id = d.account_id
        where 1=1
            and coalesce(s.is_flagged,0) = 0
            and s.is_entitled_l1 = '1'
            --and s.total_stream_time_ms_l1 > 0
            and s.ds between '2021-03-26' and current_date() - 1
        group by cube(1)
    )
)
,

first_Streams as (
    select account_id
        ,  coalesce(m.partnerseriesid, m.programid) as content_unit_id
        ,  max(coalesce(m.series_full_title,m.program_full_title)) as content_title
    from dss_prod.disney_plus.fact_disney_signup_first_watches fdsfw
        join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA m
            on m.programid = fdsfw.first_stream_program_id
    where 
        1=1 
        and m.CONTENTTYPE in ('full')
        and m.content_class in ('movie','series', 'short-form')
        and first_stream_date_Est between '2021-03-26' and current_date() - 1
    group by 1, 2
)
,

account_level_engagement as (
    select a.*
        ,  fs.account_id as first_stream_account_id
    from (
        select s.account_id
            ,  m.content_unit_id
            ,  m.studio
            ,  m.window_type
            ,  case
                    when d.account_id is null then 'Unknown'
                    when d.demo_segment_with_gender in ('4) households w/o children and male account holder', '3) households w/o children and female account holder') 
                        and account_holder_age_predict in ('18-24', '25-34') then account_holder_gender_predict || ', 18-34'
                    when d.demo_segment_with_gender in ('4) households w/o children and male account holder', '3) households w/o children and female account holder') 
                        and account_holder_age_predict in ('35-44', '45-54', '55-64', '65+') then account_holder_gender_predict || ', 35+'
                    when d.demo_segment_with_gender = '2) households with older children' then 'HH w/ Older Children'
                    when d.demo_segment_with_gender = '1) households with young children' then 'HH w/ Young Children'
               end as demo_segment
            ,  sum(s.total_stream_time_ms_l1/3600000) as account_title_streaming_hours
            ,  account_title_streaming_hours / sum(account_title_streaming_hours) over (partition by s.account_id) as pct_account_attr_sub
        from "DSS_PROD"."DISNEY_PLUS"."DIM_DISNEY_ACCOUNT_SERIES_LEVEL_ENGAGEMENT" as s
            join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_DAILY_ACCOUNT_ENGAGEMENT dae  
                on s.ds = dae.ds 
                and s.account_id = dae.account_id
            join metadata as m
                on s.content_unit_id = m.content_unit_id
            join dss_prod.disney_plus.disney_demo_fully_assigned as d
                on s.account_id = d.account_id
        where 
            1=1
            and dae.IS_ENTITLED_L1 = '1'
            and coalesce(s.account_is_flagged,0) = 0
            and s.total_streams_l1 > 0
            and s.ds between '2021-03-26' and current_date() - 1
        group by 1, 2, 3, 4, 5
    ) a 
        left join first_Streams fs 
            on fs.account_id = a.account_id
            and fs.content_unit_id = a.content_unit_id
)
,

engagement_rollup as (
    select studio
        ,  window_type
        ,  coalesce(content_unit_id, 'All Titles') as content_id
        ,  coalesce(demo_segment, 'All Subs') as demo 
        ,  num_actives
        ,  num_hours
        ,  num_first_streams
        ,  attributable_subs
    from (
        select studio
            ,  window_type
            ,  content_unit_id
            ,  demo_segment
            ,  sum(pct_account_attr_sub) as attributable_subs
            ,  count(distinct account_id) as num_actives
            ,  sum(account_title_streaming_hours) as num_hours
            ,  count(distinct first_stream_account_id) as num_first_streams
        from account_level_engagement
        group by 1, 2, cube(3, 4)
    )
)


select e.studio
    ,  coalesce(m.content_title, 'All Titles') as title
    ,  e.content_id
    ,  m.content_segment
    ,  m.content_type
    ,  e.window_type

    ,  e.demo 

    ,  num_actives
    ,  total_entitled_accounts
    ,  num_actives / total_entitled_accounts as pct_reach

    ,  num_hours
    ,  num_hours / sum(case when title = 'All Titles' then num_hours else 0 end) over (partition by e.demo) as pct_hours

    ,  num_first_streams
    ,  num_first_streams / sum(case when title = 'All Titles' then num_first_streams else 0 end) over (partition by e.demo) as pct_first_streams

    ,  attributable_subs
    ,  attributable_subs / sum(case when title = 'All Titles' then attributable_subs else 0 end) over (partition by e.demo) as pct_attr_subs
from engagement_rollup e 
    join subs s 
        on s.demo = e.demo
    left join metadata m 
        on m.content_unit_id = e.content_id
order by e.demo, pct_attr_subs desc
;


















with metadata as (
    select coalesce(m.partnerseriesid, m.programid) as content_unit_id
        ,  case
                when m.content_class = 'series' then 'Series'
                when m.content_class = 'movie' then 'Movie' 
                else 'Short-Form'
           end as content_type
        ,  studio_level_two as studio
        ,  max(case when m.is_disney_plus_original = 1 then 'Original' else 'Library' end) as window_type
        ,  max(ss.segment_level_a_name) as content_segment
        ,  max(coalesce(m.series_full_title,m.program_full_title)) as content_title
    from DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA as m
        join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_STUDIO_GROUPINGS_v sg
            on sg.content_unit_id = coalesce(m.partnerseriesid, m.programid)
        join DSS_PROD.DPLUS_ANALYTICS.DIM_DISNEY_CONTENT_SEGMENTS as ss
            on ss.CONTENT_UNIT_ID = coalesce(m.partnerseriesid, m.programid)
    where 
        1=1
        and CONTENTTYPE in ('full')
        and m.series_full_title = 'The Simpsons'
    group by 1, 2, 3
)


        select content_title
            ,  count(distinct s.account_id)
            ,  sum(s.total_stream_time_ms_l1/3600000) as account_title_streaming_hours
        from "DSS_PROD"."DISNEY_PLUS"."DIM_DISNEY_ACCOUNT_SERIES_LEVEL_ENGAGEMENT" as s
            join metadata as m
                on s.content_unit_id = m.content_unit_id
            join dss_prod.disney_plus.disney_demo_fully_assigned as d
                on s.account_id = d.account_id
        where 
            1=1
            and coalesce(s.account_is_flagged,0) = 0
            and content_title = 'The Simpsons'
            and s.total_streams_l1 > 0
            and s.ds between '2021-01-01' and '2021-09-30'
            group by 1
            
            
;



select *
from "DSS_DEV"."DISNEY_PLUS"."SH_FY_DBT"
where title <> 'All Titles' 
order by demo, pct_attr_subs desc 
;











--D+ Code 
with content_metadata as (
    select programid
         , coalesce(ddcm.partnerseriesid, ddcm.programid) as content_unit_id
         , SEGMENT_LEVEL_A_NAME
         , brands
         , max(coalesce(series_full_title, program_full_title))  as content_full_title
         , max(runtime_ms) / 3600000                             as runtime_hours
    from DSS_PROD.disney_plus.dim_disney_content_metadata ddcm
             inner join dss_prod.dplus_analytics.dim_disney_content_segments cs
                        on cs.content_unit_id = coalesce(ddcm.partnerseriesid, ddcm.programid)
             inner join disney_plus.dim_disney_content_metadata_availability ddcma
                        on ddcm.mediaid = ddcma.mediaid
             left join disney_plus.dim_disney_country_region_mapping b
                        on ddcma.region = b.alpha_code_2   
    where 
        1=1
        and ddcma.region = 'US'
        and ddcm.content_class in ('series', 'movie', 'short-form')
    group by 1, 2, 3, 4
)


select content_unit_id
    ,  content_full_title as title 
    ,  segment_level_a_name
    ,  brands
    ,  sum(runtime_hours) as runtime
from content_metadata
group by 1, 2, 3, 4
order by 1

;



select * from "DSS_DEV"."DISNEY_PLUS"."SH_FY_DBT" where title <> 'All Titles' 


;





select count(distinct content_title) from (
   select coalesce(m.partnerseriesid, m.programid) as content_unit_id
        ,  case
                when m.content_class = 'series' then 'Series'
                when m.content_class = 'movie' then 'Movie' 
                else 'Short-Form'
           end as content_type
        ,  brands
        ,  max(case when m.is_disney_plus_original = 1 then 'Original' else 'Library' end) as window_type
        ,  max(ss.segment_level_a_name) as content_segment
        ,  max(coalesce(m.series_full_title,m.program_full_title)) as content_title
    from DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA as m
        left join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_STUDIO_GROUPINGS_v sg
            on sg.content_unit_id = coalesce(m.partnerseriesid, m.programid)
        left join DSS_PROD.DPLUS_ANALYTICS.DIM_DISNEY_CONTENT_SEGMENTS as ss
            on ss.CONTENT_UNIT_ID = coalesce(m.partnerseriesid, m.programid)
    where 
        1=1
        and CONTENTTYPE in ('full')
        and brands =  'Star'
    group by 1, 2, 3
)
;





set month_end = '2021-12-31';
----------GLOBAL

CREATE OR REPLACE TABLE "DSS_DEV"."DISNEY_PLUS"."GLOBAL_CURRENT_CALENDAR_YEAR_CONTENT_AGG" AS

with dates as (
    select d.date
    from "DSS_PROD"."DISNEY_PLUS"."DIM_DATES" d
    where 
        1 = 1
        and d.date between date_trunc('year', to_Date($month_end)) and $month_end
    group by 1
)
,

metadata as(
    select coalesce(month_Date, last_day(to_Date($month_end), 'year')) as month
        ,  content_id as content_unit_id
        ,  genre
        ,  brands
        ,  launch as release_year
        ,  content_class
        ,  is_original
        ,  content_title
        ,  sum(avail_hour) as avail_hours
    from(
        select date_trunc('month', to_Date(d.date)) as month_Date
            ,  case when mm.local_id is not null then global_id else coalesce(m.partnerseriesid, m.programid) end as content_id
            ,  brands
            ,  m.content_class
            ,  case when IS_DISNEY_PLUS_ORIGINAL = 'TRUE' then 'Original' else 'Non-Original' end as is_original_flag
            ,  case when mm.local_id is not null then global_id else m.programid end as program_id
            ,  release_year as year
            ,  max(segment_level_a_name) as genre
            ,  min(year) over (partition by content_id) as launch
            ,  max(is_original_flag) over (partition by content_id) as is_original
            ,  max(runtime_ms) / 3600000 as avail_hour
            ,  max(coalesce(m.series_full_title,m.program_full_title)) as content_title
        from DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA m
            join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_AVAILABILITY a
                on a.MEDIAID = m.MEDIAID
            join dss_prod.dplus_analytics.dim_disney_content_segments ds 
                on ds.content_unit_id = coalesce(m.partnerseriesid, m.programid)
            join dates d 
                on d.date between (case when a.premieraccess_APPEARS_IN_REGION_DATE_TIME_EST is not null then to_date(a.premieraccess_APPEARS_IN_REGION_DATE_TIME_EST) else to_date(a.APPEARS_IN_REGION_DATE_TIME_EST) end) and $month_end
            left join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_CONTENT_UNIT_ID_MAPPING_V mm 
                on mm.local_id = coalesce(m.partnerseriesid, m.programid)
        where 
            1=1
            and m.CONTENTTYPE in ('full')
            and m.content_class in ('movie','series', 'short-form')
            --and a.region = 'US'
            --and series_full_title ilike '%falcon and the winter%'
        group by cube(1), 2, 3, 4, 5, 6, 7
    )
    where 
        1=1
        --and content_title ilike '%Raya and the Last Dragon%'
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by content_title
)
,

first_Streams as (
    select account_id
        ,  first_stream_date_Est
        ,  case when mm.local_id is not null then global_id else coalesce(m.partnerseriesid, m.programid) end as content_unit_id
    from dss_prod.disney_plus.fact_disney_signup_first_watches fdsfw
        join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA m
            on m.programid = fdsfw.first_stream_program_id
        left join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_CONTENT_UNIT_ID_MAPPING_V mm 
            on mm.local_id = coalesce(m.partnerseriesid, m.programid)
        join dates d 
            on first_stream_date_Est = d.date
    where 
        1=1 
        --and ACCOUNT_HOME_COUNTRY = 'US'
        and m.CONTENTTYPE in ('full')
        and m.content_class in ('movie','series', 'short-form')
        and first_stream_date_Est between date_trunc('year', to_Date($month_end)) and $month_end
    group by 1, 2, 3
)
,

engagement as (
    select coalesce(month_Date, last_day(to_Date($month_end), 'year')) as month
        ,  content_id as content_unit_id
        ,  hours
        ,  actives
        ,  first_stream
    from(
        select date_trunc('month', s.ds) as month_Date
            ,  case when mm.local_id is not null then global_id else s.content_unit_id end as content_id
            ,  sum(s.total_stream_time_ms_l1 / 3600000) as hours
            ,  count(distinct s.account_id) as actives
            ,  count(fs.account_id) as first_stream
        from "DSS_PROD"."DISNEY_PLUS"."DIM_DISNEY_ACCOUNT_SERIES_LEVEL_ENGAGEMENT" s
            join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_DAILY_ACCOUNT_ENGAGEMENT dae  
                on s.ds = dae.ds 
                and s.account_id = dae.account_id
            left join first_Streams fs 
                on s.account_id = fs.account_id
                and s.ds = fs.first_stream_date_Est
                and s.content_unit_id = fs.content_unit_id
            join dates d 
                on d.date = s.ds
            left join DSS_PROD.DISNEY_PLUS.DIM_DISNEY_CONTENT_METADATA_CONTENT_UNIT_ID_MAPPING_V mm 
                on mm.local_id = s.content_unit_id
        where 
            1=1
            and s.total_streams_l1 > 0
            --and dae.IS_ENTITLED_L1 = '1'
            --and s.ACCOUNT_HOME_COUNTRY in ('US')
            and coalesce(account_is_flagged,0) = 0
            and s.ds between date_trunc('year', to_Date($month_end)) and $month_end
        group by cube(1), 2
    )
)
,

entitled_subs as(
    select coalesce(month_Date, last_day(to_Date($month_end), 'year')) as month
        ,  entitled_accounts
    from(
        select date_trunc('month', dae.ds) as month_Date
            ,  count(distinct dae.account_id) as entitled_accounts
        from DSS_PROD.DISNEY_PLUS.DIM_DISNEY_DAILY_ACCOUNT_ENGAGEMENT dae 
            join dates d 
                on d.date = dae.ds
        where 
            1=1
            and dae.IS_ENTITLED_L1 = '1'
            --and dae.ACCOUNT_HOME_COUNTRY in ('US')
            and coalesce(is_flagged,0) = 0
            and dae.ds between date_trunc('year', to_Date($month_end)) and $month_end
        group by cube(1)
    )
)
,

engagement_rollup as (
    select month
        ,  content_unit_id
        ,  hours
        ,  actives
        ,  first_stream

        ,  sum(first_stream) over (partition by month) as monthly_first_stream
        ,  sum(hours) over (partition by month) as monthly_hours

        ,  max(actives) over (partition by content_unit_id) as max_actives
        ,  max(hours) over (partition by content_unit_id) as max_hours
        ,  max(first_stream) over (partition by content_unit_id) as max_first_streams
    from engagement

)

select er.month
    ,  m.genre
    ,  m.is_original
    ,  m.content_title
    ,  m.release_year
    ,  m.brands
    ,  m.content_class
    ,  m.avail_hours

    ,  actives
    ,  actives / nullif(entitled_accounts, 0) as reach
    ,  actives / nullif(max_actives, 0) as pct_yearly_title_Actives

    ,  first_stream
    ,  first_stream / nullif(monthly_first_stream, 0) as pct_monthy_first_streams
    ,  first_stream / nullif(max_first_streams, 0) as pct_yearly_title_first_streams

    ,  hours 
    ,  hours / nullif(monthly_hours, 0) as pct_monthly_hours
    ,  hours / nullif(max_hours, 0) as pct_yearly_title_hours
    ,  hours / nullif(avail_hours, 0) as volume_efficiency

from engagement_rollup er 
    join entitled_subs es 
        on es.month = er.month
    join metadata m 
        on m.month = er.month 
        and m.content_unit_id = er.content_unit_id
where 
    1=1
order by 4, 1
;
