from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
import argparse 
from pyspark.sql.functions import from_unixtime 
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender

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

    def __init__(self, spark_session, df_heartbeat,groupby_ids) -> None: 
        self.spark = spark_session 
        self.df_heartbeat = df_heartbeat
        self.groupby_ids = groupby_ids
        self.df_left = self.df_heartbeat.select(groupby_ids).distinct()
        self.df_currentNetwork = self.createCurrentNetwork()
        self.df_numerical = self.createNumAvg()
        self.df_result = self.df_left.join(self.df_currentNetwork, self.groupby_ids, "left")\
                                    .join(self.df_numerical, self.groupby_ids, "left")\

    def createCurrentNetwork(self, df_heartbeat = None, groupby_ids = None):
        # Create CurrentNetwork Features -----------------------------------------------------------------------------------------------------
        if groupby_ids is None:
            groupby_ids = self.groupby_ids
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat

        feature = "CurrentNetwork"
        window_spec = Window.partitionBy(groupby_ids).orderBy("time") 

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
                                    .groupby(groupby_ids)\
                                    .agg( 
                                        sum("switch_count").alias("switch_count_sum"),
                                        )
        
        return df_currentNetwork

    def createNumAvg(self, df_heartbeat= None, groupby_ids = None):
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        if groupby_ids is None:
            groupby_ids = self.groupby_ids
        df_numerical = df_heartbeat.filter( col("cpe_5gsnr")!="0" )\
                    .filter( col("snr")!="0" )
        df_numerical = convert_string_numerical(df_numerical, ["brsrp","snr","cpe_5gsnr"])\
                            .filter( col("BRSRP")> -155.0 )\
                            .filter( col("BRSRP")< 0 )\
                            .filter( col("SNR")< 55.0 )\
                            .filter( col("SNR")> -50.0 )\
                            .filter( col("cpe_5gsnr")> -50.0 )\
                            .groupby(groupby_ids)\
                                        .agg( 
                                            F.collect_list("BRSRP").alias("BRSRP_list"),
                                            F.collect_list("cpe_5gsnr").alias("cpe_5gsnr_list"),
                                            F.collect_list("SNR").alias("SNR_list")
                                            )\
                                        .withColumn("log_avg_BRSRP", get_BRSRP_Avg(F.col("BRSRP_list")))\
                                        .withColumn("log_avg_5GSNR", get_BRSRP_Avg(F.col("cpe_5gsnr_list")))\
                                        .withColumn("log_avg_SNR", get_BRSRP_Avg(F.col("SNR_list")))\
                                        .drop("BRSRP_list","cpe_5gsnr_list","SNR_list")
        return df_numerical

if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('5gHome_oma')\
            .config("spark.sql.adapative.enabled","true")\
            .config("spark.ui.port","24040")\
            .enableHiveSupport().getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    mail_sender = MailSender()

    backfill_range = 7
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args_date = parser.parse_args().date
    date_list = [( datetime.strptime( args_date, "%Y-%m-%d" )  - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(backfill_range)][::-1]

    for date_str in date_list: 
        try:    
            spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/oma_result/"+ date_str)
        except Exception as e:
            print(e)
            try:
                            
                d = date_str
                df_crsp_id = spark.read.option("header","true").csv( hdfs_pa + f"/user/kovvuve/owl_history_v3/date={d}" )\
                                .dropDuplicates()\
                                .select(
                                        col("IMEI").alias("imei"), 
                                        col("IMSI").alias("imsi")
                                        )\
                                .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))\
                                .groupby("imei","imsi").count()

                base_path = hdfs_pa + f"/user/heartbeat7min/parsed/dt={d}"  
                oma_features = [ "imei","imsi","currentnetwork","brsrp","snr","cpe_5gsnr","time","timestamp"]
                groupby_ids = ["imei","imsi"] #groupby_ids = ["imei","imsi","mdn","modelname"]

                df_oma = spark.read.option("recursiveFileLookup", "true").json(base_path)
                df_oma = df_oma.join( df_crsp_id, groupby_ids, "left_anti" )\
                                .select(oma_features)

                ins1 = heartbeat( spark_session = spark, 
                                    df_heartbeat = df_oma,
                                    groupby_ids = groupby_ids)

                ins1.df_result.repartition(10)\
                            .write.mode("overwrite")\
                            .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/oma_result/" + d )
                            #

            except Exception as e:
                print(e)
                mail_sender.send( send_from ="oma_result@verizon.com", 
                                    subject = f"oma_result failed !!! at {date_str}", 
                                    text = e)