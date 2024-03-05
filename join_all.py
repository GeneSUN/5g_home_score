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
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    days_before = 2
    d = ( date.today() - timedelta(days_before) ).strftime("%Y-%m-%d")
    
    # 1. cust_line --------- --------- --------- --------- --------- --------- --------- --------- ---------
    df_cust = spark.read.option("recursiveFileLookup", "true").option("header", "true")\
                    .csv(hdfs_pa + "/user/kovvuve/EDW_SPARK/cust_line/"+d)\
                    .withColumnRenamed("VZW_IMSI", "imsi")\
                    .withColumnRenamed( "mtn","mdn_5g")\
                    .withColumn("imei", F.expr("substring(IMEI, 1, length(IMEI)-1)"))\
                    .withColumn("cpe_model_name", F.split( F.trim(F.col("device_prod_nm")), " "))\
                    .withColumn("cpe_model_name", F.col("cpe_model_name")[F.size("cpe_model_name") -1])\
                .select("cust_id","imei","imsi","mdn_5g","pplan_cd","pplan_desc","cpe_model_name")
    # after filter TECH_GEN == "5G", 1757944/3148538 left

    for col_name in df_cust.columns: 
        df_cust = df_cust.withColumnRenamed(col_name, col_name.lower()) 
    # 2. cpe_daily_data_usage --------- --------- --------- --------- --------- --------- --------- ---------
    
    p = hdfs_pd + "/usr/apps/vmas/cpe_daily_data_usage/" + d
    df_datausage = spark.read.option("header","true").csv(p)\
                        .select("cust_id","imei","imsi","mdn_5g","fourg_total_mb","fiveg_total_mb","fiveg_usage_percentage")
    
    df_5g = df_cust.join(df_datausage.drop("cust_id","imei"),["imsi","mdn_5g"], "left" )

    df_postgre = spark.read.parquet( hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + d)\
                        .select(F.col("mdn").alias("mdn_5g"),"imei",
                                F.round(col("downloadresult"), 0).alias("downloadresult"),
                                F.round(col("uploadresult"), 0).alias("uploadresult"),
                                F.round(col("latency"), 0).alias("latency"),
                                "status","progress")\
                        .filter( col("progress") == 100)
    
    df_5g = df_5g.join(df_postgre.drop("imei","status","progress"),"mdn_5g", "left" )

    # 4. crsp_result
    df_crsp = spark.read.parquet( hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/" + d )\
                    .select("sn","imei","imsi",col("mdn").alias("mdn_5g"),
                            "ServicetimePercentage",
                            "switch_count_sum",
                            F.round("avg_CQI",2).alias("avg_CQI"),
                            F.round("avg_MemoryPercentFree",2).alias("avg_MemoryPercentFree"),
                            F.round("log_avg_BRSRP",2).alias("log_avg_BRSRP"),
                            F.round("log_avg_SNR",2).alias("log_avg_SNR"),
                            F.round("log_avg_5GSNR",2).alias("log_avg_5GSNR"),
                            F.round("LTERACHFailurePercentage",2).alias("LTERACHFailurePercentage"),
                            "LTEHandOverFailurePercentage","NRSCGChangeFailurePercentage"
                            )\
                    .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))\
                    .withColumn("mdn_5g", F.expr("substring(mdn_5g, 2, length(mdn_5g))"))

    df_5g = df_5g.join(df_crsp,["imei", "imsi", "mdn_5g"], "left" )

    df_5g.write.mode("overwrite")\
        .parquet( hdfs_pd + "/user/ZheS//5g_homeScore/join_df/" + (date.today() - timedelta(1)).strftime("%Y-%m-%d") )
    














