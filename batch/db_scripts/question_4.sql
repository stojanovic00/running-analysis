-- Most attended marathons per  distance group
SELECT
    year_of_event,
    event_name,
    distance_group_km,
    athlete_num,
    rank
FROM (
    SELECT
        year_of_event,
        event_name,
        distance_group_km,
        count(*) AS athlete_num,
        DENSE_RANK() OVER (PARTITION BY distance_group_km ORDER BY count(*) DESC) AS rank
    FROM
        public.running_distance_constrained AS rdc
    WHERE
        distance_group_km IS NOT NULL
    GROUP BY
        year_of_event,
        event_name,
        distance_group_km
) RankedResults
WHERE
    rank <= 10
ORDER BY
    CAST(SPLIT_PART(distance_group_km, '-', 1) AS INTEGER) ASC,
    rank ASC;
