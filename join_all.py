from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender


if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('jdbc_query_example')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()
        #
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    days_before = 1
    d = ( date.today() - timedelta(days_before) ).strftime("%Y-%m-%d")
    try:    
        # 1.1. cust_line --------- --------- --------- --------- --------- --------- --------- --------- ---------
        df_cust = spark.read.option("recursiveFileLookup", "true").option("header", "true")\
                        .csv(hdfs_pa + "/user/kovvuve/EDW_SPARK/cust_line/"+d)\
                        .withColumnRenamed("VZW_IMSI", "imsi")\
                        .withColumnRenamed( "mtn","mdn_5g")\
                        .withColumn("imei", F.expr("substring(IMEI, 1, length(IMEI)-1)"))\
                        .withColumn("cpe_model_name", F.split( F.trim(F.col("device_prod_nm")), " "))\
                        .withColumn("cpe_model_name", F.col("cpe_model_name")[F.size("cpe_model_name") -1])\
                    .select("cust_id","imei","imsi","mdn_5g","cpe_model_name")
        #.select("cust_id","imei","imsi","mdn_5g","pplan_cd","pplan_desc","cpe_model_name")
        # after filter TECH_GEN == "5G", 1757944/3148538 left

        for col_name in df_cust.columns: 
            df_cust = df_cust.withColumnRenamed(col_name, col_name.lower()) 

        # 1.2. cust_line --------- --------- --------- --------- --------- --------- --------- --------- ---------
        df_tracfone = spark.read.option("header","true").csv( hdfs_pa + "/user/kovvuve/tracfone/tracfone_v3.csv")\
                            .withColumnRenamed("DEVICE_ID","imei")\
                            .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))\
                            .withColumnRenamed("IMSI_VZ","imsi")\
                            .withColumnRenamed("MDN","mdn_5g")\
                            .withColumn("cust_id", F.lit("tracfone"))\
                            .withColumn("cpe_model_name", F.lit("tracfone"))

        df_id = df_cust.select("cust_id","imei","imsi","mdn_5g","cpe_model_name")\
                    .union( df_tracfone.select("cust_id","imei","imsi","mdn_5g","cpe_model_name") )

        # 2. cpe_daily_data_usage --------- --------- --------- --------- --------- --------- --------- ---------
        
        p = hdfs_pd + "/usr/apps/vmas/cpe_daily_data_usage/" + d
        df_datausage = spark.read.option("header","true").csv(p)\
                            .select("cust_id","imei","imsi","mdn_5g","fourg_total_mb","fiveg_total_mb","fiveg_usage_percentage")
        
        df_5g = df_id.join(df_datausage.drop("cust_id","imei"),["imsi","mdn_5g"], "left" )
        
        # 3. speed_test --------- --------- --------- --------- --------- --------- --------- ---------
        
        df_postgre = spark.read.parquet( hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + d)\
                            .select(F.col("mdn").alias("mdn_5g"),"imei",
                                    F.round(col("downloadresult"), 0).alias("downloadresult"),
                                    F.round(col("uploadresult"), 0).alias("uploadresult"),
                                    F.round(col("latency"), 0).alias("latency"),
                                    "status","progress")\
                            .filter( col("progress") == 100)
        
        df_5g = df_5g.join(df_postgre.drop("imei","status","progress"),"mdn_5g", "left" )

        # 4.1. crsp_result
        df_crsp = spark.read.parquet( hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/" + d )\
                        .select("imei","imsi",
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
                        .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))
        # 4.2. crsp_result
        df_oma = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/oma_result/"+d)
        crsp_columns = [col for col in df_crsp.columns if col not in df_oma.columns] 
        for col_name in crsp_columns: 
            df_oma = df_oma.withColumn(col_name, lit(None)) 
        union_df = df_crsp.union(df_oma.select(df_crsp.columns)) 

        df_5g = df_5g.join(union_df,["imei", "imsi"] )

        df_5g.write.mode("overwrite")\
            .parquet( hdfs_pd + "/user/ZheS//5g_homeScore/join_df/" + d )
        
        mail_sender.send( send_from ="5gHomeScoreJoinAll@verizon.com", 
                    subject = f"5gHomeScoreJoinAll success !!! at {d}", 
                    text = "success")
    except Exception as e:
        
        mail_sender.send( send_from ="5gHomeScoreJoinAll@verizon.com", 
                            subject = f"5gHomeScoreJoinAll failed !!! at {d}", 
                            text = e)


    














