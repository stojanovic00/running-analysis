-- Average speed per age group per distance group
SELECT 
  distance_group_km,
  age_group,
  athlete_num,
  avg_athlete_speed_kmph
FROM
  (SELECT
    distance_group_km,
    age_group,
    count(*) as athlete_num,
    avg(athlete_average_speed_kmph) AS avg_athlete_speed_kmph
  from
    public.running_distance_constrained rdc 
  where
    distance_group_km != '0-40' and
    age_group is not NULL and
    age_group != 'None' and
    avg_athlete_speed_kmph > 0
  group by 
    distance_group_km,
    age_group)
ORDER BY
    CAST(SPLIT_PART(distance_group_km, '-', 1) AS INTEGER) asc,
    CAST(SPLIT_PART(age_group, '-', 1) AS INTEGER) asc;


