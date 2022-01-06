with last_sunday_date as 
(
    select max(weekenddate) as last_sunday
    from CONTENT_MART_SHARE.DEFAULT.EST_DAY
    where 
        1=1
        and weekenddate <= current_Date
)
,

currents as
(
    select last_day(to_date(calendar_date),'week') as sunday_date
        ,  content_title
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
        join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
            on a.content_partner_id = c.content_partner_id
        join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
            on a.bundle_package_sk = p.bundle_package_sk 
            and p.content_source_group = 'SVOD'
    where 
        1=1
        and calendar_date between '2020-09-07' and (select * from last_sunday_Date)
        and cla_current_status = 'current'
    group by 1, 2
)
,

video_id_license_status as -- get all status
(
    select last_day(to_date(calendar_date),'week') as sunday_date
        ,  content_title
        ,  a.video_id
        ,  cla_current_status
        ,  content_partner_name
        ,  channel
        ,  license_type
    from "UNIVERSE360"."CONTENT"."FACT_USER_CONTENT_CONSUMPTION_DAY" a
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
            and v.programming_type in ('Full Episode','Full Movie')
        join "UNIVERSE360"."CONTENT"."DIM_CONTENT_PARTNER" c
            on a.content_partner_id = c.content_partner_id
        join "UNIVERSE360"."CONTENT"."DIM_BUNDLE_PACKAGE" p
            on a.bundle_package_sk = p.bundle_package_sk 
            and p.content_source_group = 'SVOD'
    where 
        1=1
        and calendar_date between '2020-09-07' and (select * from last_sunday_Date)
    group by 1,2,3,4,5,6,7
)
,

content_license_grouping as ( --assign status to video
    select
    *
    from(
        select v.sunday_date
            ,  v.content_title
            ,  v.video_id
            ,  case
                   when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and channel not in ('FX','FXX'))) then 'Current'
                   when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 'Exclusives'
                   when c.content_title is not null then 'Prior' 
                   else 'Library'
               end as content_type
            ,  case
                   when (content_partner_name = 'NS CP Disney_FX_Currents' or (v.cla_current_status = 'current' and channel not in ('FX','FXX'))) then 1
                   when (content_partner_name = 'NS CP Disney_FX_Exclusives' or license_type = 'original') then 3
                   when c.content_title is not null then 2
                   else 4
               end as content_type_rank
            ,  min(content_type_rank) over (partition by v.sunday_date, v.content_title, v.video_id) as rank_use
        from video_id_license_status v
            left join currents c 
                on v.content_title = c.content_title
                and v.sunday_date = c.sunday_date
        where 
            1 = 1
        group by 1,2,3,4,5
        order by 1,2
    )
    where rank_use = content_type_rank
)

select weekend_sat
    ,  network
    ,  content_title
    ,  'ALL' AS CLA_status
    ,  coalesce(content_type,'ALL') as license_type
    ,  num_content_ocm
    ,  num_content_reco
    ,  num_content_editorial
    ,  num_content_total
    ,  sum(num_content_ocm) over (partition by weekend_sat,cla_status,license_type,network) as week_network_ocm_total
    ,  sum(num_content_reco) over (partition by weekend_sat,cla_status,license_type,network) as week_network_reco_total
    ,  sum(num_content_editorial) over (partition by weekend_sat,cla_status,license_type,network) as week_network_editorial_total
    ,  sum(num_content_total) over (partition by weekend_sat,cla_status,license_type,network) as week_network_total_total
    ,  sum(num_content_ocm) over (partition by weekend_sat,cla_status,license_type) as week_ocm_total
    ,  sum(num_content_reco) over (partition by weekend_sat,cla_status,license_type) as week_reco_total
    ,  sum(num_content_editorial) over (partition by weekend_sat,cla_status,license_type) as week_editorial_total
    ,  sum(num_content_total) over (partition by weekend_sat,cla_status,license_type) as week_total_total
from (
    select last_day(to_date(a.calendar_date),'week') as weekend_sat
        ,  case
                when v.channel in ('FX', 'FXX') then 'FX'
                else v.channel
            end as network
        ,   v.content_title
        ,   clg.content_type
        ,   sum(ocm_impressions) as num_content_ocm
        ,   sum(reco_impressions) as num_content_reco
        ,   sum(editorial_impressions) as num_content_editorial
        ,   sum(total_impressions) as num_content_total
    from "UNIVERSE360"."CONTENT"."FACT_USER_IMPRESSION_DAY" a
        join "UNIVERSE360"."CONTENT"."DIM_VIDEO" v 
            on a.video_id = v.video_id
        left join content_license_grouping clg 
            on clg.video_id = a.video_id
            and clg.sunday_date = last_day(to_date(a.calendar_date),'week')
    where 
        1=1
        and content_source_group = 'SVOD'
        and a.calendar_date between '2020-09-07' and (select last_sunday from last_sunday_date)
    group by 1,2,3,cube(4)
)