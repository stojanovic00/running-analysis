-- Total distance crossed per decade in time constrained marathons
SELECT
  TO_DATE('1.1.' || (FLOOR((year_of_event - 1) / 10) * 10)::TEXT, 'DD.MM.YYYY') AS decade_start,
  SUM(distance_crossed_km) AS total_distance
FROM
  running_time_constrained rtc
GROUP BY
  decade_start
