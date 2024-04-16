from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 

        
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('jdbc_query_example')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
        #
        
    url = 'jdbc:postgresql://cospvmaspa7.nss.vzwnet.com:5432/fwa' 
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    properties = { 
                    'user': 'postgres',  
                    'password': 'adminpassword',  
                    'driver': 'org.postgresql.Driver' 
                } 

    query = """ 
    (SELECT * 
    FROM speedtest_vmb_v2  
    WHERE TO_TIMESTAMP(transactioncreatedate, 'Mon DD, YYYY hh:mi:ss PM') >= CURRENT_DATE - INTERVAL '1 days' 
    AND TO_TIMESTAMP(transactioncreatedate, 'Mon DD, YYYY hh:mi:ss PM') < CURRENT_DATE
    AND status <> 'FAILED'
    ) AS subquery 
    """ 

    df_postgre = spark.read.jdbc(url=url, table=query, properties=properties)\
                    .withColumn("uploadresult", col("uploadresult").cast("double"))\
                    .withColumn("downloadresult", col("downloadresult").cast("double"))\
                    .withColumn("latency", col("latency").cast("double"))
    
    df_postgre.write.mode("overwrite")\
        .parquet( hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + (date.today() - timedelta(1)).strftime("%Y-%m-%d") )
    