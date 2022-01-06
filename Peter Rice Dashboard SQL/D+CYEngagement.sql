set month_end = '2021-11-30';

--DOMESTIC
CREATE OR REPLACE TABLE "DSS_DEV"."DISNEY_PLUS"."CURRENT_CALENDAR_YEAR_CONTENT_AGG" AS
with dates as (
    select d.date
    from "DSS_PROD"."DISNEY_PLUS"."DIM_DATES" d
    where 
        1 = 1
        and d.date between date_trunc('year', current_date()) and $month_end
    group by 1
)
,

metadata as(
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
            and a.region = 'US'
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
        and ACCOUNT_HOME_COUNTRY = 'US'
        and m.CONTENTTYPE in ('full')
        and m.content_class in ('movie','series', 'short-form')
        and first_stream_date_Est between date_trunc('year', current_date()) and $month_end
    group by 1, 2, 3
)
,

engagement as (
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
            and s.ACCOUNT_HOME_COUNTRY in ('US')
            and coalesce(account_is_flagged,0) = 0
            and s.ds between date_trunc('year', current_date()) and $month_end
        group by cube(1), 2
    )
)
,

entitled_subs as(
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
            and dae.ACCOUNT_HOME_COUNTRY in ('US')
            and coalesce(is_flagged,0) = 0
            and dae.ds between date_trunc('year', current_date()) and $month_end
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













set month_end = '2021-11-30';
----------GLOBAL

CREATE OR REPLACE TABLE "DSS_DEV"."DISNEY_PLUS"."GLOBAL_CURRENT_CALENDAR_YEAR_CONTENT_AGG" AS

with dates as (
    select d.date
    from "DSS_PROD"."DISNEY_PLUS"."DIM_DATES" d
    where 
        1 = 1
        and d.date between date_trunc('year', current_date()) and $month_end
    group by 1
)
,

metadata as(
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
        and first_stream_date_Est between date_trunc('year', current_date()) and $month_end
    group by 1, 2, 3
)
,

engagement as (
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
            and s.ds between date_trunc('year', current_date()) and $month_end
        group by cube(1), 2
    )
)
,

entitled_subs as(
    select coalesce(month_Date, last_day(current_Date, 'year')) as month
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
            and dae.ds between date_trunc('year', current_date()) and $month_end
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


