-- Runners with best improvement over whole career (per distance group)
WITH AthleteFirstLastRuns AS (
    SELECT
        athlete_id,
        distance_group_km,
        MIN(event_dates) AS earliest_run,
        MAX(event_dates) AS latest_run
    FROM
        running_distance_constrained 
    GROUP BY
        athlete_id, distance_group_km
),

AthletePerformance AS (
    SELECT
        aflr.athlete_id,
        aflr.distance_group_km,
        rtc_first.athlete_average_speed_kmph AS avg_speed_first_marathon,
        rtc_last.athlete_average_speed_kmph AS avg_speed_last_marathon
    FROM
        AthleteFirstLastRuns aflr
        JOIN running_distance_constrained rtc_first
            ON aflr.athlete_id = rtc_first.athlete_id
            AND aflr.distance_group_km = rtc_first.distance_group_km
            AND aflr.earliest_run = rtc_first.event_dates
        JOIN running_distance_constrained rtc_last
            ON aflr.athlete_id = rtc_last.athlete_id
            AND aflr.distance_group_km = rtc_last.distance_group_km
            AND aflr.latest_run = rtc_last.event_dates
    WHERE 
        rtc_first.athlete_average_speed_kmph != 0 AND
        rtc_last.athlete_average_speed_kmph != 0 AND
        aflr.distance_group_km != '0-40'
),

AthletesImprovement AS (
    SELECT
        athlete_id,
        distance_group_km,
        avg_speed_first_marathon,
        avg_speed_last_marathon,
        (avg_speed_last_marathon - avg_speed_first_marathon) / avg_speed_first_marathon * 100 AS improvement_percentage
    FROM
        AthletePerformance
)

SELECT  
    athlete_id,
    distance_group_km,
    avg_speed_first_marathon,
    avg_speed_last_marathon,
    improvement_percentage,
    rank_by_improvement
FROM (
    SELECT
        athlete_id,
        distance_group_km,
        avg_speed_first_marathon,
        avg_speed_last_marathon,
        improvement_percentage,
        DENSE_RANK() OVER (PARTITION BY distance_group_km ORDER BY improvement_percentage DESC) AS rank_by_improvement
    FROM
        AthletesImprovement
) ranked_results
WHERE
    rank_by_improvement <= 10
ORDER BY
    CAST(SPLIT_PART(distance_group_km, '-', 1) AS INTEGER) ASC,
    rank_by_improvement ASC;
