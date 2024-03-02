import argparse

from pyspark.sql import SparkSession

# temporary view name for querying
temp_view ="delay_flights"

# queries for getting Avg delay times
queries = [
    {"carrier_delay_query": f"""SELECT Year, AVG(CarrierDelay) AS Avg_Carrier_Delay 
            FROM {temp_view} 
            GROUP BY Year 
            ORDER BY Year DESC"""},    
    {"nas_delay_query": f"""SELECT Year, AVG(NASDelay) AS Avg_NAS_Delay 
            FROM {temp_view} 
            GROUP BY Year 
            ORDER BY Year DESC"""},
    {"weather_delay_query": f"""SELECT Year, AVG(WeatherDelay) AS Avg_Weather_Delay 
            FROM {temp_view} 
            GROUP BY Year 
            ORDER BY Year DESC"""},
    {"late_aircraft_delay_query": f"""SELECT Year, AVG(LateAircraftDelay) AS Avg_Late_Aircraft_Delay 
            FROM {temp_view} 
            GROUP BY Year 
            ORDER BY Year DESC"""},
    {"security_delay_query": f"""SELECT Year, AVG(SecurityDelay) AS Avg_Security_Delay 
            FROM {temp_view} 
            GROUP BY Year 
            ORDER BY Year DESC"""}
]

def query_flight_data(data_source, output_uri, repeat = 1):
    """
    :param data_source: The URI of data CSV, such as 's3://DOC-EXAMPLE-BUCKET/data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/results'.
    """
    with SparkSession.builder.appName("Query Flight Delays").getOrCreate() as spark:
        if data_source is not None:
            flight_delay_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flight_delay_df.createOrReplaceTempView(temp_view)

        # run each query repititively
        for i in range(0, repeat):
            for query in queries:
                start_time = spark.sql("SELECT current_timestamp() AS start_time")
                delay_flights_query_result = spark.sql(next(iter(query.values())))
                end_time = spark.sql("SELECT current_timestamp() AS end_time")
                
                # construct output paths for timestamps & query results for each query in each iteration
                query_name = next(iter(query))
                output_path = f"{output_uri}/{query_name}/{i}"
                results_output_path = f"{output_path}/results"
                timestamp_output_path = f"{output_path}/timestamps"

                # Write the results to the specified output URI
                delay_flights_query_result.write.option("header", "true").mode("overwrite").csv(results_output_path)
                
                # write start & end timestamps
                start_time.write.option("header", "true").mode("overwrite").csv(f"{timestamp_output_path}/start_time")
                end_time.write.option("header", "true").mode("overwrite").csv(f"{timestamp_output_path}/end_time")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for CSV data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    query_flight_data(args.data_source, args.output_uri, 5)