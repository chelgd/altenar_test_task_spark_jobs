from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from os import system
from sys import argv

spark = SparkSession.builder.\
    appName('spark_test').\
    getOrCreate()

df = spark.read.format('parquet').load('/user/ubuntu/ga_parsed_jsons.parquet')

report_one = df.\
    groupBy(df['actor_login']).\
    agg(
        sf.countDistinct(df['repo_id']).alias('repo_cnt')
    ).\
    filter(sf.col('repo_cnt')>1)
report_one.coalesce(1).write.format('csv').option('header', True).save(f'/user/ubuntu/report_1-{argv[1]}.csv')

actor_date_agg = df.\
    filter(
        df['type']=='PushEvent'
    ).\
    groupBy(
        df['actor_login'],
        df['event_date']
    ).\
    agg(
        sf.count(df['event_id']).alias('event_cnt')
    ).\
    cache()

report_two = actor_date_agg.\
    filter(
        sf.col('event_cnt')>1
    ).\
    orderBy(
        actor_date_agg['actor_login'],
        actor_date_agg['event_cnt']
    )
report_two.coalesce(1).write.format('csv').option('header', True).save(f'/user/ubuntu/report_2-{argv[1]}.csv')

report_three = actor_date_agg.\
    filter(
        sf.col('event_cnt')<=1
    )
report_three.coalesce(1).write.format('csv').option('header', True).save(f'/user/ubuntu/report_3-{argv[1]}.csv')

report_five = df.\
    groupBy(
        df['repo_id'],
        df['repo_name'],
        df['event_date']
    ).\
    agg(
        sf.count(df['actor_id']).alias('actor_cnt')
    ).\
    filter(
        sf.col('actor_cnt')>10
    ).\
    groupBy(
        df['event_date']
    ).\
    agg(
        sf.count(df['repo_id']).alias('repo_cnt')
    )
report_five.coalesce(1).write.format('csv').option('header', True).save(f'/user/ubuntu/report_5-{argv[1]}.csv')