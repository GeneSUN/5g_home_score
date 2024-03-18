from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 

from pyspark.sql.functions import from_unixtime 
@udf(returnType=FloatType())
def get_BRSRP_Avg(values):

    if len(values) > 0: 

        avg_linear_scale = np.mean( np.power(10, np.array(values)/ 10) )
        #avg_linear_scale = np.power(10, np.mean(values) / 10) 
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
    def __init__(self, spark_session, df) -> None: 
        self.spark = spark_session 
        self.df = df

        self.window_spec = Window.partitionBy(ids).orderBy("time") 
        self.df_heartbeat = self.numericalDf()
        self.df_ServiceTime = self.createServiceTime()
        self.df_CurrentNetwork = self.createCurrentNetwork()
        self.df_CreateCount = self.createCount()
        self.df_groupby = self.groupbyDf()

    def numericalDf(self, df_heartbeat = None):
        if df_heartbeat is None:
            df_heartbeat = self.df
            
        df_heartbeat = df_heartbeat.dropDuplicates()\
                    .withColumn('time', F.from_unixtime(col('ts') / 1000.0).cast('timestamp'))\
                    .select("time","sn","mac","rowkey","CellID", "IMEI","MDN", "IMSI","ModelName","RebootCause",
                            "SNR","CQI", "MemoryPercentFree","BRSRP", "5GSNR","5GEARFCN_DL",
                            "ServiceDowntime","ServiceUptime",
                            "LTERACHAttemptCount","LTERACHFailureCount",
                            "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                            "NRSCGChangeCount","NRSCGChangeFailureCount",
                            "CurrentNetwork")                    

        df_heartbeat = convert_string_numerical( df_heartbeat, 
                                                ["BRSRP","SNR","5GSNR","CQI","MemoryPercentFree","5GEARFCN_DL",
                                                "LTERACHAttemptCount","LTERACHFailureCount",
                                                "LTEHandOverAttemptCount","LTEHandOverFailureCount",
                                                "NRSCGChangeCount","NRSCGChangeFailureCount"] )
        #
        df_heartbeat = df_heartbeat.filter( col("BRSRP")> -155.0 )\
                                    .filter( col("SNR")< 55.0 )\
                                    .filter( col("CQI")< 15.0 )\
                                    .filter(F.col("MemoryPercentFree").isNotNull() & ~F.isnan("MemoryPercentFree"))
        return df_heartbeat
    
    def createServiceTime(self, df_heartbeat = None):

        window_spec = self.window_spec 
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        df_ServiceTime = df_heartbeat.filter( (col("ServiceDowntime")!="184467440737095")&
                                                (col("ServiceUptime")!="184467440737095")
                                                )\
                                    .withColumn("ServiceDowntime_change", 
                                            when(col("ServiceDowntime") != F.lag("ServiceDowntime").over(window_spec), 1).otherwise(0))\
                                    .withColumn("ServiceUptime_change", 
                                            when(col("ServiceUptime") != F.lag("ServiceUptime").over(window_spec), 1).otherwise(0))
        return df_ServiceTime

    def createCurrentNetwork(self, df_heartbeat = None):
        # Create CurrentNetwork Features -----------------------------------------------------------------------------------------------------
        feature = "CurrentNetwork"
        if df_heartbeat is None:
            df_heartbeat = self.df_ServiceTime
        
        window_spec = self.window_spec 
        df_heartbeat = df_heartbeat.filter((col("CurrentNetwork").isNotNull()) & 
                                    (col("CurrentNetwork") != "-") & 
                                    (col("CurrentNetwork") != "0") & 
                                    (col("CurrentNetwork") != "12") &
                                    (col("CurrentNetwork") != "13") &
                                    (col("CurrentNetwork") != "None")
                                    )\
                                    .withColumn("prev_"+feature, F.lag(feature).over(window_spec))\
                                    .withColumn("switch_count", 
                                            F.when(F.col("prev_"+feature) != F.col(feature) , 1).otherwise(0))
        return df_heartbeat

    def createCount(self,df_heartbeat= None):
        if df_heartbeat is None:
            df_heartbeat = self.df_CurrentNetwork
        window_spec = self.window_spec 
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
        return df_heartbeat
    
    def groupbyDf(self, df_heartbeat= None):
        if df_heartbeat is None:
            df_heartbeat = self.df_CreateCount
        
        sum_columns = [F.sum("increment_" + feature).alias("sum_" + feature) for feature in count_features] 
        # Define the UDF (User Defined Function) 



        # Apply the UDF to the DataFrame 
        df_result = df_heartbeat.groupby(ids)\
                        .agg( sum("ServiceDowntime_change").alias("ServiceDowntime_sum"),
                            sum("ServiceUptime_change").alias("ServiceUptime_sum"),
                            sum("switch_count").alias("switch_count_sum"),
                            F.collect_list("BRSRP").alias("BRSRP_list"),
                            F.collect_list("5GSNR").alias("5GSNR_list"),
                            F.collect_list("SNR").alias("SNR_list"),
                            avg("CQI").alias("avg_CQI"),
                            avg("MemoryPercentFree").alias("avg_MemoryPercentFree"),
                            *sum_columns
                            )\
                        .withColumn("ServicetimePercentage", col("ServiceDowntime_sum")/(col("ServiceDowntime_sum")+col("ServiceUptime_sum") ) )\
                        .withColumn("LTERACHFailurePercentage",col("sum_LTERACHFailureCount")/col("sum_LTERACHAttemptCount"))\
                        .withColumn("LTEHandOverFailurePercentage",col("sum_LTEHandOverFailureCount")/col("sum_LTEHandOverAttemptCount"))\
                        .withColumn("NRSCGChangeFailurePercentage",col("sum_NRSCGChangeFailureCount")/col("sum_NRSCGChangeCount"))\
                        .withColumn("log_avg_BRSRP", get_BRSRP_Avg(F.col("BRSRP_list")))\
                        .withColumn("log_avg_5GSNR", get_BRSRP_Avg(F.col("5GSNR_list")))\
                        .withColumn("log_avg_SNR", get_BRSRP_Avg(F.col("SNR_list")))\
                        .drop("BRSRP_list","5GSNR_list","SNR_list")

        return df_result

if __name__ == "__main__":
    
    spark = SparkSession.builder\
            .appName('5gHome_crsp')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    #for i in range(15):
    day_before = 1
    d = ( date.today() - timedelta(day_before) ).strftime("%Y-%m-%d")

    df = spark.read.option("header","true").csv( hdfs_pa + f"/user/kovvuve/owl_history_v3/date={d}" )

    ins1 = heartbeat( spark_session = spark, 
                        df = df)

    ins1.df_groupby.repartition(10)\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/" + d )
                #