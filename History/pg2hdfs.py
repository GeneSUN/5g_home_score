from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
import argparse 
        
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('jdbc_query_example')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
        #

    day_before = 1
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(day_before) ).strftime("%Y-%m-%d")) 
    args = parser.parse_args()

    d = args.date
    url = 'jdbc:postgresql://cospvmaspa7.nss.vzwnet.com:5432/fwa' 
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    properties = { 
                    'user': 'postgres',  
                    'password': 'adminpassword',  
                    'driver': 'org.postgresql.Driver' 
                } 

    query = f""" 
    (SELECT * 
    FROM speedtest_vmb_v2  
    WHERE TO_TIMESTAMP(transactioncreatedate, 'Mon DD, YYYY hh:mi:ss PM') > DATE '{d}'  
    AND TO_TIMESTAMP(transactioncreatedate, 'Mon DD, YYYY hh:mi:ss PM') <= DATE '{d}' + INTERVAL '1 days' 
    AND status <> 'FAILED'
    ) AS subquery 
    """ 

    df_postgre = spark.read.jdbc(url=url, table=query, properties=properties)\
                    .withColumn("uploadresult", col("uploadresult").cast("double"))\
                    .withColumn("downloadresult", col("downloadresult").cast("double"))\
                    .withColumn("latency", col("latency").cast("double"))
    
    df_postgre.write.mode("overwrite")\
        .parquet( hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + d )
    