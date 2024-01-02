CREATE TABLE running_distance_constrained (
	id SERIAL PRIMARY key,
    year_of_event INTEGER,
    event_dates VARCHAR(255),
    event_name VARCHAR(255),
    event_number_of_finishers INTEGER,
    time_spent VARCHAR(255),
    athlete_club VARCHAR(255),
    athlete_country VARCHAR(255),
    athlete_year_of_birth INTEGER,
    athlete_gender VARCHAR(255),
    athlete_id INTEGER,
    athlete_age INTEGER,
    distance_km DOUBLE PRECISION,
    time_spent_s INTEGER,
    athlete_average_speed_kmph DOUBLE PRECISION
);

