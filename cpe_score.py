from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,percentile_approx,explode
from pyspark.sql.functions import udf 
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import numpy as np
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
import argparse 
from functools import reduce

def convert_to_numeric(df, col_name):
    return df.withColumn(f"{col_name}_numeric", F.when(F.col(col_name) == "Poor", 1)
                                                .when(F.col(col_name) == "Fair", 2)
                                                .when(F.col(col_name) == "Good", 3)
                                                .when(F.col(col_name) == "Excellent", 4)
                                                .otherwise(None))
def convert_to_categorical(df, col_name):
    return df.withColumn(col_name, 
                        F.when(F.col(col_name) < 1.5, "Poor")
                        .when((F.col(col_name) >= 1.5) & (F.col(col_name) < 2.5), "Fair")
                        .when((F.col(col_name) >= 2.5) & (F.col(col_name) < 3.5), "Good")
                        .when(F.col(col_name) >= 3.5, "Excellent")
                        .otherwise(None))

class ScoreCalculator: 
    def __init__(self, weights): 
        self.weights = weights 
 
    def calculate_score(self, *args): 
        total_weight = 0 
        score = 0 

        for weight, value in zip(self.weights.values(), args): 
            if value is not None: 
                score += weight * float(value) 
                total_weight += weight 

        return score / total_weight if total_weight != 0 else None 
    
class CellularScore:
    global hdfs_pa, hdfs_pd, count_features

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    count_features = ["LTERACHFailureCount", "LTEHandOverFailureCount", "NRSCGChangeFailureCount","RRCConnectFailureCount"]
    def __init__(self,d): 
        self.d = d
        self.df_heartbeat = spark.read.option("header","true").csv( hdfs_pa + f"/user/kovvuve/owl_history_v3/date={self.d}" )\
                                .dropDuplicates()\
                                .withColumn('time', F.from_unixtime(col('ts') / 1000.0).cast('timestamp'))
        self.custline_path = hdfs_pa + "/user/kovvuve/EDW_SPARK/cust_line/"+ self.d
        self.df_price_cap = self.get_price_plan_df()
        self.df_cust = self.get_customer_df()
        self.df_throughput = self.get_throughput_df()
        self.df_linkCapacity = self.get_linkCapacity_df()
        self.df_ServiceTime = self.get_ServiceTime_df()
        self.df_score = self.get_score_df()

    def get_price_plan_df(self):
        price_plan_data = [
            ('67577', 50, 6), ('50011', 50, 6), ('38365', 50, 6), ('50010', 50, 6), ('75565', 50, 6), 
            ('65655', 50, 6), ('67584', 50, 6), ('65656', 50, 6), ('67571', 100, 10), ('50128', 300, 20), 
            ('50127', 300, 20), ('75561', 300, 20), ('67576', 300, 20), ('50130', 300, 20), ('50129', 300, 20), 
            ('67567', 400, 20), ('50044', 400, 20), ('50116', 1500, 75), ('67568', 1500, 75), ('75560', 1500, 75)
        ]

        columns = ['PPLAN_CD', 'DL_CAP', 'UL_CAP']

        df_price_cap = spark.createDataFrame(price_plan_data, columns)
        return df_price_cap

    def get_customer_df(self, custline_path = None):
        if custline_path is None:
            custline_path = self.custline_path

        cpe_models_to_keep = ["ARC-XCI55AX", "ASK-NCQ1338FA", "WNC-CR200A", "ASK-NCQ1338", "FSNO21VA", "NCQ1338E", "ASK-NCM1100E", "ASK-NCM1100", "Others"]
        cpe_models_to_keep = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'WNC-CR200A', "CR1000A", "CR1000B"]
        #fiveg_pplan = ['51219', '27976', '53617', '50044', '50055', '50127', '50128', '67571', '67567', '50129', '67576', '67568', '50116', '50117', '50130', '39425', '39428']
        #fourg_pplan = ['48390', '48423', '48445', '46799', '46798', '50010', '50011', '67577', '38365', '67584', '65655', '65656']

        df_cust = spark.read.option("recursiveFileLookup", "true").option("header", "true")\
                        .csv(custline_path)\
                        .withColumnRenamed("VZW_IMSI", "IMSI")\
                        .withColumnRenamed("MTN", "MDN_5G")\
                        .withColumn("IMEI", F.expr("substring(IMEI, 1, length(IMEI)-1)"))\
                        .withColumn("CPE_MODEL_NAME", F.split(F.trim(F.col("DEVICE_PROD_NM")), " "))\
                        .withColumn("CPE_MODEL_NAME", F.col("CPE_MODEL_NAME")[F.size("CPE_MODEL_NAME") - 1])\
                        .withColumn("CPE_MODEL_NAME", 
                                    when(F.col("CPE_MODEL_NAME").isin(cpe_models_to_keep), F.col("CPE_MODEL_NAME")) 
                                        .otherwise("Others"))\
                        .dropDuplicates()
        
        return df_cust
    
    def get_throughput_df(self, df_cust = None,df_price_cap = None):
        if df_cust is None:
            df_cust = self.df_cust
        if df_price_cap is None:
            df_price_cap = self.df_price_cap

        ultra_schema = StructType([
            StructField("IMSI", StringType(), True),
            StructField("DEVICE_MODEL", StringType(), True),
            StructField("UE_OVERALL_DL_SPEED", DoubleType(), True)
        ])

        # Try to read the CSV and handle the case where it might not exist
        try:
            df_ultra = spark.read.option("header", "true")\
                .csv(hdfs_pa + f"/fwa/npp_mdn_agg_insights_rtt/datadate={self.d}")\
                .select("IMSI", 'DEVICE_MODEL', 'UE_OVERALL_DL_SPEED')\
                .filter(F.col("UE_OVERALL_DL_SPEED").isNotNull())\
                .filter(F.col("UE_OVERALL_DL_SPEED") != 0)\
                .groupBy('IMSI', 'DEVICE_MODEL')\
                .agg(F.max('UE_OVERALL_DL_SPEED').alias('UE_OVERALL_DL_SPEED'))
        except Exception as e:
            mail_sender.send(
                    send_from="cellular_Score@verizon.com",
                    subject=f"ultragauge missed at {self.d}",
                    text=e
                )
            df_ultra = spark.createDataFrame([], ultra_schema)  # Create an empty DataFrame if not exists

        # Continue with df_ultra as normal
        df_ultrag_price_cap = df_ultra.join(df_cust.select("IMSI", "MDN_5G", "PPLAN_CD", "PPLAN_DESC", "CPE_MODEL_NAME"), "IMSI", "right")\
            .join(df_price_cap, on="PPLAN_CD")\
            .filter(F.col("UE_OVERALL_DL_SPEED").isNotNull())\
            .filter(F.col("UE_OVERALL_DL_SPEED") != 0)\
            .withColumn(
                "ULTRAGAUGE_DL_SCORE",
                F.round(
                    F.when((F.col("UE_OVERALL_DL_SPEED") / F.col("DL_CAP") * 2) > 1, 1)
                    .otherwise(F.col("UE_OVERALL_DL_SPEED") / F.col("DL_CAP") * 2), 4)
            )\
            .withColumn(
                "ULTRAGAUGE_DL_SCORE", col("ULTRAGAUGE_DL_SCORE")*100
            )\
            .select("MDN_5G", "IMSI", "CPE_MODEL_NAME", "PPLAN_CD", "UE_OVERALL_DL_SPEED", "DL_CAP", "UL_CAP", "ULTRAGAUGE_DL_SCORE")


        try:
            speedtest_path = hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + self.d
            df_speedtest = spark.read.parquet(speedtest_path)\
                                .filter(F.col("progress") == 100)\
                                .filter(F.col("DOWNLOADRESULT").isNotNull())\
                                .filter(F.col("DOWNLOADRESULT")!=0)\
                                .filter(F.col("UPLOADRESULT").isNotNull())\
                                .filter(F.col("UPLOADRESULT")!=0)\
                                .select(F.col("mdn").alias("MDN_5G"),
                                        F.round(F.col("downloadresult"), 0).alias("DOWNLOADRESULT"),
                                        F.round(F.col("uploadresult"), 0).alias("UPLOADRESULT"),
                                        F.round(F.col("latency"), 0).alias("LATENCY"),
                                        )\
                                .groupby("MDN_5G")\
                                .agg( F.avg("DOWNLOADRESULT").alias("DOWNLOADRESULT"),
                                        F.avg("UPLOADRESULT").alias("UPLOADRESULT"),
                                )\
                                .dropDuplicates()
        except:

            # Create an empty DataFrame with the same schema
            empty_schema = StructType([
                StructField("MDN_5G", StringType(), True),
                StructField("DOWNLOADRESULT", DoubleType(), True),
                StructField("UPLOADRESULT", DoubleType(), True)
            ])
            df_speedtest = spark.createDataFrame([], empty_schema)
        
        df_throughput = df_ultrag_price_cap.join(df_speedtest, "MDN_5G", "left")\
                                .withColumn(
                                    "Download_Score", 
                                    F.round(
                                            F.when((F.col("DOWNLOADRESULT") / F.col("DL_CAP")) > 1, 1)
                                            .otherwise(F.col("DOWNLOADRESULT") / F.col("DL_CAP")), 
                                            4) )\
                                .withColumn(
                                    "Upload_Score", 
                                    F.round(F.when((F.col("UPLOADRESULT") / F.col("UL_CAP")) > 1, 1)
                                     .otherwise(F.col("UPLOADRESULT") / F.col("UL_CAP")), 4) )\
                                .withColumn(
                                    "Download_Score", col("Download_Score")*100)\
                                .withColumn(
                                    "Upload_Score", col("Upload_Score")*100)
        
        return df_throughput

    def get_linkCapacity_df(self, df_heartbeat = None, df_cust = None, df_price_cap = None):
        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        if df_cust is None:
            df_cust = self.df_cust
        if df_price_cap is None:
            df_price_cap = self.df_price_cap

        df_heartbeat = df_heartbeat.join(df_cust.select("IMSI", "MDN_5G", "PPLAN_CD", "PPLAN_DESC", "CPE_MODEL_NAME"), "IMSI", "right")\
                                    .join(df_price_cap, on="PPLAN_CD")\

        df_with_bandwidths = df_heartbeat.withColumnRenamed("SNR", "_4gsnr").withColumnRenamed("5GSNR", "_5gsnr")\
                                        .filter(
                                                    (F.col("_4gsnr").between(-10, 40)) & (F.col("_4gsnr") != 0) & 
                                                    (F.col("_5gsnr").between(-10, 40)) & (F.col("_5gsnr") != 0)
                                                )\
                                        .withColumn(
                                            "_lte_band",
                                            (F.when(F.col("4GPccBand").cast("bigint") > 0, 20).otherwise(0) +
                                            F.when(F.col("4GScc1Band").cast("bigint") > 0, 20).otherwise(0) +
                                            F.when(F.col("4GScc2Band").cast("bigint") > 0, 20).otherwise(0) +
                                            F.when(F.col("4GScc3Band").cast("bigint") > 0, 20).otherwise(0))
                                        ).withColumn(
                                            "_nwbandwidth",
                                            (F.when((F.col("5GPccBand").cast("bigint") > 0) & (F.col("5GPccBand").cast("bigint") != 77), 20).otherwise(0) +
                                            F.when((F.col("5GScc1Band").cast("bigint") > 0) & (F.col("5GScc1Band").cast("bigint") != 77), 20).otherwise(0))
                                        ).withColumn(
                                            "_cbandbandwidths",
                                            F.when(
                                                (F.col("5GPccBand").cast("bigint") == 77) & (F.col("5GScc1Band").cast("bigint") == 77), 160
                                            ).when(
                                                (F.col("5GPccBand").cast("bigint") == 77) & (F.col("5GEARFCN_DL").between(646667, 653329)), 100
                                            ).when(
                                                (F.col("5GPccBand").cast("bigint") == 77) & (~F.col("5GEARFCN_DL").between(646667, 653329)), 60
                                            ).when(
                                                (F.col("5GPccBand").cast("bigint") != 77) & (F.col("5GScc1Band").cast("bigint") == 77), 80
                                            ).otherwise(0)
                                        )

        df_linkCapacity = df_with_bandwidths.filter(
                                        ( F.col("_lte_band") + F.col("_nwbandwidth") + F.col("_cbandbandwidths")) > 0
                                        )\
                                        .withColumn(
                                            "lte_capacity",
                                            F.round(
                                                F.when(
                                                    F.col("_4gsnr") == 0, 0
                                                ).otherwise(
                                                    F.col("_lte_band") * F.least(F.lit(1), (F.col("_4gsnr") + 11) / 41.0)
                                                ), 2
                                            )
                                        ).withColumn(
                                            "nw_capacity",
                                            F.round(
                                                F.when(
                                                    F.col("_5gsnr") == 0, 0
                                                ).otherwise(
                                                    F.col("_nwbandwidth") * F.least(F.lit(1), (F.col("_5gsnr") + 11) / 41.0)
                                                ), 2
                                            )
                                        ).withColumn(
                                            "c_band_capacity",
                                            F.round(
                                                F.when(
                                                    F.col("_5gsnr") == 0, 0
                                                ).otherwise(
                                                    F.col("_cbandbandwidths") * 0.8 * F.least(F.lit(1), (F.col("_5gsnr") + 10) / 41.0)
                                                ), 2
                                            )
                                        )\
                                        .withColumn(
                                                "Rate_Plan_Adjustment", 
                                                F.least(F.col("DL_CAP") / 150, F.lit(1.0))
                                            )\
                                        .withColumn(
                                            "_capacity", 
                                            F.round(
                                                100*((F.col("lte_capacity") + F.col("nw_capacity") + F.col("c_band_capacity")) /  (218*col("Rate_Plan_Adjustment") ) ),
                                                2)
                                            )\
                                        .withColumn(
                                            "_capacity", 
                                            F.round(
                                                        F.when((F.col("_capacity")) > 100, 100)
                                                        .otherwise(F.col("_capacity")), 
                                                    4) )\
                                        .groupby("sn","imsi")\
                                        .agg( 
                                            F.round(F.avg("lte_capacity"),2).alias("lte_capacity"), 
                                            F.round(F.avg("nw_capacity"),2).alias("nw_capacity"), 
                                            F.round(F.avg("c_band_capacity"),2).alias("c_band_capacity"), 
                                            F.round(F.avg("Rate_Plan_Adjustment"),2).alias("Rate_Plan_Adjustment"), 
                                            F.round(F.avg("_capacity"),2).alias("capacity_score") 
                                            )\
                                        .withColumn( "capacity_score_category", when(col("capacity_score") >= 80, "Excellent")
                                                                                .when(col("capacity_score") >= 50, "Good")
                                                                                .when(col("capacity_score") >= 30, "Fair")
                                                                                .otherwise("Poor") )
                                        

        return df_linkCapacity

    def get_ServiceTime_df(self, df_heartbeat = None):

        if df_heartbeat is None:
            df_heartbeat = self.df_heartbeat
        
        window_spec = Window.partitionBy("sn").orderBy("ServiceUptime") 

        df_heartbeat = df_heartbeat.filter( (col("ServiceDowntime")!="184467440737095")&
                                                (col("ServiceUptime")!="184467440737095")
                                                )\
                                    .withColumn("ServiceDowntime_change", 
                                            when(col("ServiceDowntime") != F.lag("ServiceDowntime").over(window_spec), 1).otherwise(0))\
                                    .withColumn("_ServiceUptime_change", 
                                            when(col("ServiceUptime") == F.lag("ServiceUptime").over(window_spec), 1).otherwise(0))\
                                    .withColumn("ServiceUptime_change", 
                                            when(col("ServiceUptime") != F.lag("ServiceUptime").over(window_spec), 1).otherwise(0))
                                            
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
                                    *sum_columns,
                                    sum("ServiceDowntime_change").alias("ServiceDowntime_sum"),
                                    sum("ServiceDowntime_change").alias("_ServiceUptime_sum"),
                                    sum("ServiceUptime_change").alias("ServiceUptime_sum"),
                                    )\
                                .withColumn("ServicetimePercentage", 100*col("_ServiceUptime_sum")/(col("_ServiceUptime_sum")+col("ServiceUptime_sum") ) )\
                                .withColumn( "assumed_downtime", F.col("sum_RRCConnectFailureCount") * 1 + F.col("sum_LTERACHFailureCount") * 0.01 + F.col("_ServiceUptime_sum")*300 )\
                                .withColumn(
                                    "not_available_percentage",
                                    (F.col("assumed_downtime") / ( 24*60*60 )) * 100
                                )\
                                .withColumn( "availability_score",
                                            F.when(
                                                (100 - 20 * F.col("not_available_percentage") ) < 0, 
                                                0 
                                            ).otherwise(
                                                F.round(100 - 20 * F.col("not_available_percentage") , 2) 
                                            )
                                        )\
                                .withColumn( "availability_score_category", when(col("availability_score") == 100, "Excellent")
                                                .when(col("availability_score") >= 99.77, "Good")
                                                .when(col("availability_score") >= 97.22, "Fair")
                                                .otherwise("Poor") )


        return df_count
    
    def get_score_df(self, df_throughput = None, df_linkCapacity = None, df_ServiceTime = None):
        if df_throughput is None:
            df_throughput = self.df_throughput
        if df_linkCapacity is None:
            df_linkCapacity = self.df_linkCapacity
        if df_ServiceTime is None:
            df_ServiceTime = self.df_ServiceTime

        df_join = df_throughput.join(df_linkCapacity, "imsi" )\
                                .join(df_ServiceTime, "sn" )

        throughput_score_weights = {
                                    "ultragauge_dl_score": 28,
                                    "download_score": 4,
                                    "upload_score": 1,
                                }
        throughput_score_calculator = ScoreCalculator(throughput_score_weights)
        throughput_score_udf = udf(throughput_score_calculator.calculate_score, FloatType())

        from pyspark.sql.functions import sum, lag, col
        df_score = df_join.withColumn(
                                        "throughput_score",
                                        F.round(
                                            throughput_score_udf(*[col(column) for column in throughput_score_weights.keys()]), 
                                            2
                                        )
                                    )\
                        .withColumn( "throughput_score_category", when(col("throughput_score") >= 80, "Excellent")
                                                        .when(col("throughput_score") >= 60, "Good")
                                                        .when(col("throughput_score") >= 30, "Fair")
                                                        .otherwise("Poor") )\


        categorical_columns = [
                                    "throughput_score_category", "capacity_score_category", "availability_score_category",]

        for col_name in categorical_columns:
            df_score = convert_to_numeric(df_score, col_name) #"{col_name}_numeric"


        score_weights = {
                            "availability_score_category_numeric": 5,
                            "capacity_score_category_numeric": 2,
                            "throughput_score_category_numeric": 1,
                        }
        score_calculator = ScoreCalculator(score_weights)
        score_udf = udf(score_calculator.calculate_score, FloatType())

        df_score = df_score.withColumn(
                                        "score",
                                        F.round(
                                            score_udf(*[col(column) for column in score_weights.keys()]), 
                                            2
                                        )
                                    )
        
        df_score = convert_to_categorical(df_score, "score")

        return df_score


if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('cpe_Score_ZheS')\
            .config("spark.sql.adapative.enabled","true")\
            .config("spark.ui.port","24041")\
            .enableHiveSupport().getOrCreate()
    
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    date_str = (date.today() - timedelta(1) ).strftime("%Y-%m-%d")

    ins = CellularScore(d = date_str)
    #ins.df_ServiceTime.show(5)
    ins.df_score.write.mode("overwrite").parquet(f"/user/ZheS/cpe_Score/all_score/{date_str}")

    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA',"ARC-XCI55AX", 'WNC-CR200A', "ASK-NCM1100"]
    ins.df_score.filter( col("CPE_MODEL_NAME").isin( models )  )\
                .write.mode("overwrite").parquet(f"/user/ZheS/cpe_Score/titan_score/{date_str}")

