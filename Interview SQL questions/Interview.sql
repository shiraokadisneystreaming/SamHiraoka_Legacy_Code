--Inital. Screen
​
playback                              video
+---------------+---------+           +---------------+---------+
| subscriber_id | varchar | +-------->| id            | int     |
| date          | date    | |         | content_type  | varchar | (Series, Film)
| video_id      | int     |-+         | length_type   | varchar |
| profile_id    | int     |-----+     | series_title  | varchar |
| device_id     | int     |--+  |     | video_title   | varchar |
| playback_time | int     |  |  |     | season_number | int     |
| views         | int     |  |  |     | episode_number| int     |
+---------------+---------+  |  |     +---------------+---------+
                             |  |
device                       |  |     profile
+---------------+---------+  |  |     +---------------+---------+
| id            | int     |<-+  +---->| id            | int     |
| category      | varchar |           | first_name    | varchar |
| type          | varchar |           | gender        | varchar |
| manufacturer  | varchar |           | date_of_birth | date    |
+---------------+---------+           +---------------+---------+
​
​
Definitions:
​
Watch: Playback of 3 minutes or more
​
​
*/
​
-- EASY: -- How many (unique) subscribers watched season 2 of 'Castle Rock' in 2020?
SELECT
  COUNT(DISTINCT a.subscriber_id) as unique_subs
FROM playback a
INNER JOIN video b ON a.video_id = b.id
WHERE
  YEAR(a.date) = 2020 AND
  b.series_title = 'Castle Rock' AND
  b.season_number = 2
;
​




-- EASY/MEDIUM: -- What percent of subscribers that have started 'Little Fires Everywhere' have watched all 10 episodes?
​
SELECT
  SUM(CASE WHEN episode_count = 10 THEN 1 ELSE 0 END)*1.0/SUM(1) as pct_complete
FROM
(
  SELECT
    a.subscriber_id,
    COUNT(DISTINCT episode_number) episode_count
  FROM playback a
  INNER JOIN video b ON a.video_id = b.id
  WHERE
    b.series_title = 'Little Fires Everywhere'
  GROUP BY
    a.subscriber_id
)
;
​




​
-- EASY/MEDIUM: What are the top 10  Series and top 10 Movies by playback hours in March 2021 (assume playback_time is milliseconds)
​
SELECT
  content_type,
  series_title,
  hours_watched,
  row_number() over(PARTITION by content_type order by hours_watched desc) as rank
from
  (
​
    SELECT
        b.content_type,
        b.series_title,
        sum(a.playback_time/3600000) hours_watched
      FROM playback a
      INNER JOIN video b ON a.video_id = b.id
      WHERE
        a.date between '2021-03-01' and '2021-03-31'
      GROUP BY
        b.content_type,
        b.series_title
  )
having rank <= 10
​








​
-- MEDIUM: What percent of subscribers that have started 'Castle Rock' have watched only the first episode of season 1?
​
SELECT
  SUM(CASE WHEN ep1 = 1 AND episode_count = 1 THEN 1 ELSE 0 END)*1.0/SUM(1) as pct_complete
FROM
(
  SELECT
    a.subscriber_id,
    MAX(CASE WHEN episode_number = 1 and season_number = 1 THEN 1 ELSE 0 END) ep1,
    COUNT(DISTINCT season_number||episode_number) episode_count
  FROM playback a
  INNER JOIN video b ON a.video_id = b.id
  WHERE
    b.series_title = 'Castle Rock'
  GROUP BY
    a.subscriber_id
)
;
​




-- HARD: -- What are the top 5 other shows watched on each device type in 2020 by those who watched 'Little Fires Everywhere' in 2020?
​
WITH t1 as
(
  SELECT
    v.series_title,
    d.device_type,
    SUM(playback_time) as pb
  FROM playback p
  INNER JOIN
  (
    SELECT
      DISTINCT subscriber_id
    FROM playback a
    INNER JOIN video b ON a.video_id = b.id
    WHERE
      YEAR(date) = 2020 AND
      b.series_title = 'Little Fires Everywhere'
  ) a ON p.subscriber_id = a.subscriber_id
  INNER JOIN video v ON p.video_id = v.id
  INNER JOIN device d ON p.device_id = d.id
  WHERE
    v.series_title != 'Little Fires Everywhere'
  GROUP BY
    v.series_title,
    d.device_type
)
​
SELECT *
FROM
(
  SELECT
    series_title,
    device_type,
    RANK() OVER(PARTITION BY series_title, device_type ORDER BY pb DESC) as rnk
  FROM t1
)
WHERE
  rnk <= 5
ORDER BY
  device_type,
  rnk
;







--Loop Screen

/*
Schema:

playback                              video
+---------------+---------+           +---------------+---------+
| subscriber_id | varchar | +-------->| id            | int     |
| date          | date    | |         | content_type  | varchar | (Film vs Series)
| video_id      | int     |-+         | video_length  | int     |
| profile_id    | int     |           | series_title  | varhcar |
| device_id     | int     |           | video_title   | varchar |
| playback_time | int     |           | season_number | int     |
| views         | int     |           | episode_number| int     |
+---------------+---------+           +---------------+---------+
                             
* playback_time is in minutes
* video_length is in minutes
* date is at day level

*/



--1a.) How many unique subscribers streamed Family Guy in 2020 and what were the total hours streamed?
--1b.) what if I wanted to see this by day/week/month?
select date
    ,  count(distinct subscriber_id) as unique_Streams
    ,  sum(playback_time / 60) as hours
from playback p 
    join video v 
        on p.video_id = v.id
where 1=1
      and year(date) = 2020
      and series_title = 'Family Guy'

group by 1




--2.) What were the top 10 Films and top 10 Series by hours for subscribers who streamed Family Guy?
with family_guy_streamers as (
    select subscriber_id
    from playback p 
        join video v 
            on p.video_id = v.id
    where 1=1
          and series_title = 'Family Guy'
    group by 1
)

select *
from (
    select content_type
        ,  series_title
        ,  sum(playback_time) as minutes
        ,  rank() over (partition by content_type order by minutes desc) as stream_rank
    from playback p 
        join video v 
            on p.video_id = v.id
        join family_guy_streamers fgs 
            on fgs.subscriber_id = p.subscriber_id
    where 1=1
          and series_title = 'Family Guy'
    group by 1, 2
)
where stream_rank <= 10


--3.) How many subscribers first playback event was Family Guy?
--  (Assume you can only have one playback event on a single day)
select count(distinct subscriber_id) as num_first_Watches
from (
    select subscriber_id
        ,  series_title
        ,  min(date) as first_watch
        ,  rank() over (partition by subscriber_id order by first_watch) as watch_event_order
    from playback p 
        join video v 
            on p.video_id = v.id
    where 1=1
    group by 1, 2
)
where 1=1
      and series_title = 'Family Guy'
      and watch_event_order = 1


--4.) For each subscriber, only count the number of episodes that were watched 90% of the way through
with video_metadata as (
    select id
        ,  video_length
    from video 
    where series_title = 'Family Guy'
    group by 1, 2
)


select subscriber_id
    ,  video_id
from (
    select subscriber_id
        ,  video_id
        ,  sum(playback_time) as video_hours
    from playback p 
        join video v 
            on p.video_id = v.id
    where 1=1
          and series_title = 'Family Guy'
    group by 1, 2
) a 
    join video_metadata vm 
        on a.video_id = vm.id 
where 1=1
      and video_hours / video_length >= .9
group by 1, 2



--5.) How many subscribers streamed all episodes of Family Guy?

with video_metadata as (
    select series_title
        ,  count(distinct season_number||episode_number) as total_episodes
    from video 
    where series_title = 'Family Guy'
    group by 1
)


select count(distinct subscriber_id) as total_streamers
from (
    select subscriber_id
        ,  series_title
        ,  count(distinct season_number||episode_number) as episodes_streamed
    from playback p 
        join video v 
            on p.video_id = v.id
    where 1=1
          and series_title = 'Family Guy'
    group by 1, 2
) a 
    join video_metadata vm 
        on a.series_title = vm.series_title 
where 1=1
      and episodes_streamed = total_episodes
