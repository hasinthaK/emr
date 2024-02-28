-- Create Table Schema with needed columns
CREATE EXTERNAL TABLE IF NOT EXISTS delay_flights (
    Year INT,
    Month INT,
    DayofMonth INT,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    LateAircraftDelay INT,
    SecurityDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Load the data from csv to the Hive table
LOAD DATA LOCAL INPATH '${INPUT}' INTO TABLE delay_flights;

-- Run desired query & write output to the given location
-- write query start & end times for each query
SET hivevar:output = '${OUTPUT}/${hivevar:iteration}';

INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/carrier_delay_query/timestamps/start_time' SELECT unix_timestamp(current_timestamp()) as start_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/carrier_delay_query/results' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Year, AVG(CarrierDelay) AS Avg_Carrier_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/carrier_delay_query/timestamps/end_time' SELECT unix_timestamp(current_timestamp()) as end_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/nas_delay_query/timestamps/start_time' SELECT unix_timestamp(current_timestamp()) as start_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/nas_delay_query/results' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Year, AVG(NASDelay) AS Avg_NAS_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/nas_delay_query/timestamps/end_time' SELECT unix_timestamp(current_timestamp()) as end_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/weather_delay_query/timestamps/start_time' SELECT unix_timestamp(current_timestamp()) as start_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/weather_delay_query/results' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Year, AVG(WeatherDelay) AS Avg_Weather_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/weather_delay_query/timestamps/end_time' SELECT unix_timestamp(current_timestamp()) as end_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/late_aircraft_delay_query/timestamps/start_time' SELECT unix_timestamp(current_timestamp()) as start_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/late_aircraft_delay_query/results' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Year, AVG(LateAircraftDelay) AS Avg_Late_Aircraft_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/late_aircraft_delay_query/timestamps/end_time' SELECT unix_timestamp(current_timestamp()) as end_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/security_delay_query/timestamps/start_time' SELECT unix_timestamp(current_timestamp()) as start_time;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/security_delay_query/results' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT Year, AVG(SecurityDelay) AS Avg_Security_Delay FROM delay_flights GROUP BY Year ORDER BY Year DESC;
INSERT OVERWRITE DIRECTORY '${OUTPUT}/${hivevar:iteration}/security_delay_query/timestamps/end_time' SELECT unix_timestamp(current_timestamp()) as end_time;
