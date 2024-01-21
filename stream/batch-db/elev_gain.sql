--CREATE TABLE elev_gain_total (
--    time TIMESTAMP,
--    value FLOAT
--);

--CREATE TABLE altitude_gain (
--    time TIMESTAMP,
--    value FLOAT
--);

--CREATE OR REPLACE FUNCTION update_elev_gain_total()
--RETURNS TRIGGER AS $$
--BEGIN
--    -- Update the elev_gain_total table with the maximum date and sum of values
--    INSERT INTO elev_gain_total (time, value)
--    SELECT NOW(), SUM(value)
--    FROM altitude_gain;
--
--    RETURN NEW;
--END;
--$$ LANGUAGE plpgsql;
--
--CREATE TRIGGER update_elev_gain_trigger
--AFTER INSERT ON altitude_gain
--FOR EACH ROW
--EXECUTE FUNCTION update_elev_gain_total();

-- ### RAW TESTING ###
--INSERT INTO altitude_gain (time, value)
--VALUES ('2021-01-21 10:36:00', 5.0);

truncate avg_speed;
truncate pace;
truncate comparison;
truncate elev_gain_total;
truncate altitude_gain ;
truncate finish_estimation ;



