from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
sys.path.append('/usr/apps/vmas/script/ZS') 
from MailSender import MailSender

from pyspark.sql.functions import from_unixtime 


def convert_string_numerical(df, String_typeCols_List): 
    """ 
    This function takes a PySpark DataFrame and a list of column names specified in 'String_typeCols_List'. 
    It casts the columns in the DataFrame to double type if they are not in the list, leaving other columns 
    as they are. 

    Parameters: 
    - df (DataFrame): The PySpark DataFrame to be processed. 
    - String_typeCols_List (list): A list of column names not to be cast to double. 

    Returns: 
    - DataFrame: A PySpark DataFrame with selected columns cast to double. 
    """ 
    # Cast selected columns to double, leaving others as they are 
    df = df.select([F.col(column).cast('double') if column in String_typeCols_List else F.col(column) for column in df.columns]) 
    return df
        
if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    #
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    d = ( date.today() - timedelta(5) ).strftime("%Y-%m-%d")

    df_heartbeat = spark.read.option("header","true").csv( hdfs_pa + f"/user/kovvuve/owl_history_v2/date={d}" )\
                        .dropDuplicates()\
                        .withColumn('time', from_unixtime(col('ts') / 1000.0).cast('timestamp'))\
                        .select("time","sn","mac","rowkey",
                                "ServiceDowntime","ServiceUptime","CurrentNetwork",
                                "BRSRP","SNR","CQI",
                                "LTERACHAttemptCount","LTERACHFailureCount",
                                "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                                "NRSCGChangeCount","NRSCGChangeFailureCount")
                        

    df_heartbeat = convert_string_numerical( df_heartbeat, ["BRSRP","SNR","CQI",
                                "LTERACHAttemptCount","LTERACHFailureCount",
                                "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                                "NRSCGChangeCount","NRSCGChangeFailureCount"] )
    
    #df_heartbeat.filter( col("sn") == "GRR13800079").orderBy("time").show(300)
    for column_name in ["LTERACHAttemptCount","LTERACHFailureCount",
                                "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                                "NRSCGChangeCount","NRSCGChangeFailureCount"]:

        summary_df = df_heartbeat.select(column_name).summary("mean", "stddev", "5%", "25%", "50%", "75%", "95%") 
        summary_df = summary_df.toDF(*[column_name] + [f"{column_name}_{stat}" for stat in summary_df.columns[1:]]) 
        summary_df.show() 
    sys.exit()

    def count_network_change(df_heartbeat):
        window_spec = Window().partitionBy("rowkey").orderBy("time")
        df_with_change_indicator = df_heartbeat.withColumn("network_change", when(col("CurrentNetwork") != F.lag("CurrentNetwork").over(window_spec), 1).otherwise(0)) 
        result_df = df_with_change_indicator.groupBy("sn").agg(F.sum("network_change").alias("network_change_count")) 

        column_name = "network_change_count" 
        summary_df = result_df.select(column_name).summary("mean", "stddev", "5%", "25%", "50%", "75%", "95%") 
        summary_df = summary_df.toDF(*[column_name] + [f"{column_name}_{stat}" for stat in summary_df.columns[1:]]) 
        summary_df.show() 

    count_network_change(df_heartbeat)

    df_heartbeat.filter( col("sn") == "AA122102235").select("time","sn","LTEHandOverAttemptCount","LTEHandOverFailureCount","ServiceDowntime","ServiceUptime","CurrentNetwork").orderBy("time").show(300)
    df_heartbeat.filter( col("sn") == "GRR13800079").select("time","sn","LTERACHAttemptCount","LTERACHFailureCount","ServiceDowntime","ServiceUptime","CurrentNetwork").orderBy("time").show(300)
    df_heartbeat.filter( col("sn") == "AB931900649").select("time","sn","NRSCGChangeCount","NRSCGChangeFailureCount","ServiceDowntime","ServiceUptime","CurrentNetwork").orderBy("time").show(300)

    window_spec = Window().partitionBy("rowkey").orderBy("time")
    attempCount = "LTEHandOverAttemptCount"
    df_with_change_indicator = df_heartbeat.filter(col(attempCount)!= 0.0)\
                                            .withColumn(f"{attempCount}_desc", 
                                                       when(col(attempCount) < F.lag(attempCount).over(window_spec), 1).otherwise(0))
    
    df = df_with_change_indicator.groupby("rowkey").agg(sum(f"{attempCount}_desc").alias(f"{attempCount}_desc_count"))
    df.orderBy(F.desc(f"{attempCount}_desc_count")).show()
    df.describe(f"{attempCount}_desc_count").show()





    window_spec = Window().partitionBy("rowkey").orderBy("time")
    df_with_change_indicator = df_heartbeat.withColumn("ServiceDowntime_change", 
                                                       when(col("ServiceDowntime") != F.lag("ServiceDowntime").over(window_spec), 1).otherwise(0))\
                                            .withColumn("cumulative_sum", 
                                                        F.sum("ServiceDowntime_change").over(window_spec) )
    
    df_with_change_indicator.filter( (col("sn")=="GRR21813755")&(col("mac")=="C899B27BE0A0") )\
                            .orderBy("sn","mac","time","ServiceDowntime")\
                            .show()
    sys.exit()
    #df_heartbeat.filter( (col("ts")=="1706596216289")&(col("sn")=="ABU24736223")&(col("mac")=="4C22F3BCD874")&(col("rowkey")=="6223-ABU24736223_4C22F3BCD874") ).show()
    #df_heartbeat.filter( (col("sn")=="GRR14800080")&(col("mac")=="C0D7AA7544A8") ).orderBy("time").show()
    df_heartbeat.filter( (col("sn")=="ACA33428829")&(col("mac")=="AC919BA403C8") )\
                .select("sn","mac","time","ServiceDowntime","ServiceUptime")\
                .orderBy("time").show()