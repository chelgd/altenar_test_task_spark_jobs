import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

files_to_parse = sys.argv[1:]

spark = SparkSession.builder.\
    appName('spark_test').\
    getOrCreate()

for file_to_parse in files_to_parse:
    json_df = spark.read.format('json').load('/user/ubuntu/altenar_test_data/' + file_to_parse)
    
    parsed_df = json_df.\
        select(
            json_df['id'].alias('event_id'),
            json_df['type'],
            json_df['repo']['id'].alias('repo_id'), 
            json_df['repo']['name'].alias('repo_name'),
            json_df['actor']['id'].alias('actor_id'), 
            json_df['actor']['login'].alias('actor_login')
        ).\
        withColumn('event_date', sf.lit(file_to_parse[:10])).\
        withColumn('file_name', sf.lit(file_to_parse))

    parsed_df.write.\
        format('parquet').\
        option('compression', 'snappy').\
        mode('append').\
        save('/user/ubuntu/ga_parsed_jsons.parquet')