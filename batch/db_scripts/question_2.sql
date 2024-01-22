-- Marathon popularity depending on its size
select rdc.distance_group_km, count(*) num_athletes
from public.running_distance_constrained rdc 
where rdc.distance_group_km is not null and rdc.distance_group_km != '0-40'
group by distance_group_km
order by num_athletes desc;

