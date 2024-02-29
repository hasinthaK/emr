import boto3

directory_names = ["carrier_delay_query", 
                   "nas_delay_query", 
                   "weather_delay_query", 
                   "late_aircraft_delay_query", 
                   "security_delay_query"]
output_folders = ['spark_output', 'hadoop_output']

def create_s3_directories():
    boto3.setup_default_session(profile_name='learner-lab')
    s3_client = boto3.client('s3')
    
    bucket = 'emr-sources'
    
    # iterate 5 times as per the requirment in assignment
    for i in range(0, 5):
        for directory in directory_names:
            for folder in output_folders:
                st_file_name = f"{folder}/{i}/{directory}/timestamps/start_time/keep"
                end_file_name = f"{folder}/{i}/{directory}/timestamps/end_time/keep"
                results_file_name = f"{folder}/{i}/{directory}/results/keep"
                
                try:
                    s3_client.put_object(Body='', Bucket=bucket, Key=st_file_name)
                    s3_client.put_object(Body='', Bucket=bucket, Key=end_file_name)
                    s3_client.put_object(Body='', Bucket=bucket, Key=results_file_name)
                except Exception as e:
                    print(e)
                    return False

if __name__ == "__main__":
    create_s3_directories()