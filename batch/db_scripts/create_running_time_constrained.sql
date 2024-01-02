CREATE TABLE running_time_constrained (
	id SERIAL PRIMARY key,
    year_of_event INTEGER,
    event_dates VARCHAR(255),
    event_name VARCHAR(255),
    event_number_of_finishers INTEGER,
    athlete_club VARCHAR(255),
    athlete_country VARCHAR(255),
    athlete_year_of_birth INTEGER,
    athlete_gender VARCHAR(255),
    athlete_age INTEGER,
    time_limit_h INTEGER,
    distance_crossed_km DOUBLE PRECISION,
    athlete_average_speed_kmph DOUBLE PRECISION,
    athlete_id INTEGER
);

