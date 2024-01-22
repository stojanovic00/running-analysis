-- Average speed comparison between genders
select 
	distance_group_km,
	athlete_gender,
	AVG(athlete_average_speed_kmph)
from running_distance_constrained 
where
	distance_group_km != '0-40' and 
	athlete_average_speed_kmph != 0 and 
	(athlete_gender = 'F' or athlete_gender = 'M')
group by
	distance_group_km,
	athlete_gender
order by
    CAST(SPLIT_PART(distance_group_km, '-', 1) AS INTEGER) asc,
   athlete_gender;

