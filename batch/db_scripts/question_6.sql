-- Countries with most runners
select merged_t.athlete_country country, count(merged_t.ath_id) num_of_athletes
from
(
select distinct  athlete_country, athlete_id ath_id from public.running_time_constrained rtc 
union
select distinct  athlete_country,  athlete_id ath_id from public.running_distance_constrained rdc
) merged_t
group by country
order by num_of_athletes desc ;
