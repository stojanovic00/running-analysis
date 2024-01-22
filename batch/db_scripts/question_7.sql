-- Distribution of runners per age group
select merged_t.age_group , count(merged_t.ath_id) num_of_athletes
from
  (
  select distinct
    age_group,
    athlete_id ath_id from public.running_time_constrained rtc 
    where
      age_group is not null and age_group != 'None'
  union
  select distinct 
    age_group,
    athlete_id ath_id from public.running_distance_constrained rdc
    where
      age_group is not null and age_group != 'None'
  ) merged_t
group by age_group;

