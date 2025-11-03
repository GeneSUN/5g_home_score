from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
from pyspark.sql.functions import from_unixtime 
import argparse 

@udf(returnType=FloatType())
def get_BRSRP_Avg(values):

    if len(values) > 0: 

        avg_linear_scale = np.mean( np.power(10, np.array(values)/ 10) )
        avg_log_scale = np.log10(avg_linear_scale) * 10 

        return float(avg_log_scale) 

    else: 
        return None 

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

class heartbeat():
    global count_features,ids
    ids = ["sn","IMEI","MDN", "IMSI","ModelName"]
    count_features = ["LTERACHAttemptCount", "LTERACHFailureCount", "LTEHandOverAttemptCount", 
            "LTEHandOverFailureCount", "NRSCGChangeCount", "NRSCGChangeFailureCount"]


    def __init__(self, spark_session, df_heartbeat) -> None: 
        self.spark = spark_session 
        self.df_heartbeat = df_heartbeat
        self.df_heartbeat = self.numericalDf()

        self.df_left = self.df_heartbeat.select(ids).distinct()
        self.df_ServiceTime = self.createServiceTime()
        self.df_CurrentNetwork = self.createCurrentNetwork()
        self.df_CreateCount = self.createCount()
        self.df_groupby = self.createNumAvg()
        self.df_result = self.df_left.join(self.df_ServiceTime, "sn", "left")\
                                    .join(self.df_CurrentNetwork, "sn", "left")\
                                    .join(self.df_CreateCount, "sn", "left")\
                                    .join(self.df_groupby, "sn", "left") 
        
    def numericalDf(self, df_heartbeat = None):
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
            
        df_heartbeat = df_heartbeat.dropDuplicates()\
                    .withColumn('time', F.from_unixtime(col('ts') / 1000.0).cast('timestamp'))\
                    .select("time","sn","mac","rowkey","CellID", "IMEI","MDN", "IMSI","ModelName","RebootCause",
                            "SNR","5GSNR","CQI", "MemoryPercentFree","BRSRP", "4GRSRP","5GEARFCN_DL",
                            "ServiceDowntime","ServiceUptime",
                            "LTERACHAttemptCount","LTERACHFailureCount",
                            "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                            "NRSCGChangeCount","NRSCGChangeFailureCount",
                            "CurrentNetwork")                    

        df_heartbeat = convert_string_numerical( df_heartbeat, 
                                                ["4GRSRP","BRSRP","SNR","5GSNR","CQI","MemoryPercentFree","5GEARFCN_DL",
                                                "LTERACHAttemptCount","LTERACHFailureCount",
                                                "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                                                "NRSCGChangeCount","NRSCGChangeFailureCount"] )
        
        return df_heartbeat
    
    def createServiceTime(self, df_heartbeat = None):

        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        
        window_spec = Window.partitionBy("sn").orderBy("time") 

        df_ServiceTime = df_heartbeat.filter( (col("ServiceDowntime")!="184467440737095")&
                                                (col("ServiceUptime")!="184467440737095")
                                                )\
                                    .withColumn("ServiceDowntime_change", 
                                            when(col("ServiceDowntime") != F.lag("ServiceDowntime").over(window_spec), 1).otherwise(0))\
                                    .withColumn("ServiceUptime_change", 
                                            when(col("ServiceUptime") != F.lag("ServiceUptime").over(window_spec), 1).otherwise(0))\
                                    .groupby("sn")\
                                    .agg( sum("ServiceDowntime_change").alias("ServiceDowntime_sum"),
                                        sum("ServiceUptime_change").alias("ServiceUptime_sum"),
                                        )\
                                    .withColumn("ServicetimePercentage", 100*col("ServiceDowntime_sum")/(col("ServiceDowntime_sum")+col("ServiceUptime_sum") ) )

        return df_ServiceTime

    def createCurrentNetwork(self, df_heartbeat = None):
        # Create CurrentNetwork Features -----------------------------------------------------------------------------------------------------
        
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        feature = "CurrentNetwork"
        window_spec = Window.partitionBy("sn").orderBy("time") 

        df_currentNetwork = df_heartbeat.filter((col("CurrentNetwork").isNotNull()) & 
                                            (col("CurrentNetwork") != "-") & 
                                            (col("CurrentNetwork") != "0") & 
                                            (col("CurrentNetwork") != "12") &
                                            (col("CurrentNetwork") != "13") &
                                            (col("CurrentNetwork") != "None")
                                            )\
                                    .withColumn("prev_"+feature, F.lag(feature).over(window_spec))\
                                    .withColumn("switch_count", 
                                            F.when(F.col("prev_"+feature) != F.col(feature) , 1).otherwise(0))\
                                    .groupby("sn")\
                                    .agg( 
                                        sum("switch_count").alias("switch_count_sum"),
                                        )
        
        return df_currentNetwork

    def createCount(self,df_heartbeat= None):
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat

        window_spec = Window.partitionBy("sn").orderBy("time") 
        
        for feature in count_features: 
            # It is tricky of whether | filter( col(feature)!=0 ) |
            df_heartbeat = df_heartbeat\
                                    .withColumn("prev_"+feature, F.lag(feature).over(window_spec))\
                                    .withColumn("pre<cur", 
                                                F.when(F.col("prev_"+feature) <= F.col(feature) , 1).otherwise(0))\
                                    .withColumn("increment_" + feature, 
                                                F.when((F.col("pre<cur") == 1) & (F.col("prev_" + feature).isNotNull()), 
                                                    F.col(feature) - F.col("prev_" + feature)) 
                                                .otherwise(F.coalesce(F.col(feature), F.lit(0) )))
                                                
        sum_columns = [F.sum("increment_" + feature).alias("sum_" + feature) for feature in count_features] 
        df_count = df_heartbeat.groupby("sn")\
                                .agg( 
                                    *sum_columns
                                    )\
                                .withColumn("LTERACHFailurePercentage",col("sum_LTERACHFailureCount")/col("sum_LTERACHAttemptCount"))\
                                .withColumn("LTEHandOverFailurePercentage",col("sum_LTEHandOverFailureCount")/col("sum_LTEHandOverAttemptCount"))\
                                .withColumn("NRSCGChangeFailurePercentage",col("sum_NRSCGChangeFailureCount")/col("sum_NRSCGChangeCount"))
                                             
        return df_count
    
    def createNumAvg(self, df_heartbeat= None):
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat

        five_g_freq_condition = (
            col("5GEARFCN_DL").cast("numeric").between(2070833, 2084999) |
            col("5GEARFCN_DL").cast("numeric").between(2229167, 2279166) |
            col("5GEARFCN_DL").cast("numeric").between(386000, 398000) |
            col("5GEARFCN_DL").cast("numeric").between(173800, 178800) |
            col("5GEARFCN_DL").cast("numeric").between(743333, 795000) |
            col("5GEARFCN_DL").cast("numeric").between(422000, 440000) |
            col("5GEARFCN_DL").cast("numeric").between(620000, 680000)
        )
        four_g_freq_condition = ~five_g_freq_condition

        df_heartbeat = df_heartbeat.filter(     ((col("BRSRP") > -155) & (col("BRSRP") < -20)) | (col("BRSRP") == 0) )\
                                    .filter( col("SNR")< 55.0 )\
                                    .filter( col("CQI")< 15.0 )\
                                    .filter( col("5GSNR")> -23 )\
                                    .filter( col("5GSNR")< 40 )\
                                    .filter(F.col("MemoryPercentFree").isNotNull() & ~F.isnan("MemoryPercentFree"))\
                                    .withColumn("4G_SNR",when(four_g_freq_condition, col("SNR")).otherwise(None) )\
                                    .withColumn("5G_SNR",when(five_g_freq_condition, col("5GSNR")).otherwise(None) )\
                                    .withColumn("4G_RSRP",when(four_g_freq_condition, col("4GRSRP")).otherwise(None) )\
                                    .withColumn("5G_RSRP",when(five_g_freq_condition, col("BRSRP")).otherwise(None) )
        # Apply the UDF to the DataFrame 
        df_result = df_heartbeat.groupby("sn")\
                                .agg( 
                                    F.collect_list("4G_RSRP").alias("4GRSRP_list"),
                                    F.collect_list("5G_RSRP").alias("BRSRP_list"),
                                    F.collect_list("4G_SNR").alias("SNR_list"),
                                    F.collect_list("5G_SNR").alias("5GSNR_list"),

                                    count("5GEARFCN_DL").alias("total_count"),
                                    F.sum(when(five_g_freq_condition, 1).otherwise(0)).alias("five_g_count"),
                                    F.sum(when(four_g_freq_condition, 1).otherwise(0)).alias("four_g_count"),

                                    avg("CQI").alias("avg_CQI"),
                                    avg("MemoryPercentFree").alias("avg_MemoryPercentFree"),
                                    count("*").alias("count_received")
                                    )\
                                .withColumn("five_g_percentage", F.round( (col("five_g_count") / col("total_count")) * 100, 2 ))\
                                .withColumn("four_g_percentage", F.round( (col("four_g_count") / col("total_count")) * 100, 2 ) )\
                                .withColumn("log_avg_BRSRP", get_BRSRP_Avg(F.col("BRSRP_list")))\
                                .withColumn("log_avg_4GRSRP", get_BRSRP_Avg(F.col("4GRSRP_list")))\
                                .withColumn("log_avg_5GSNR", get_BRSRP_Avg(F.col("5GSNR_list")))\
                                .withColumn("log_avg_SNR", get_BRSRP_Avg(F.col("SNR_list")))\
                                .withColumn("count_received", F.when(F.col("count_received") > 230, 230).otherwise(F.col("count_received")))\
                                .withColumn("percentageReceived", F.round( F.col("count_received")/230, 4 )*100 )\
                                .drop("4GRSRP_list","BRSRP_list","5GSNR_list","SNR_list")
                                # 230 is the majority number of records for each home in heartbeat
        return df_result

if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('5gHome_crsp')\
            .config("spark.sql.adapative.enabled","true")\
            .config("spark.ui.port","24042")\
            .enableHiveSupport().getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    backfill_range = 7
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args_date = parser.parse_args().date
    date_list = [( datetime.strptime( args_date, "%Y-%m-%d" )  - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(backfill_range)][::-1]

    for date_str in date_list: 
        try:    
            spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/"+ date_str)
        except Exception as e:
            print(e)
            try:
                
                df_heartbeat = spark.read.option("header","true").csv( hdfs_pa + f"/user/kovvuve/owl_history_v3/date={date_str}" )

                ins1 = heartbeat( spark_session = spark, 
                                    df_heartbeat = df_heartbeat)

                ins1.df_result.repartition(10)\
                            .write\
                            .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/" + date_str )
                            #.mode("overwrite")
            except Exception as e:
                print(e)
                mail_sender.send( send_from ="crsp_result@verizon.com", 
                                    subject = f"crsp_result failed !!! at {date_str}", 
                                    text = e)