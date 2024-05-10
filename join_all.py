from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
import argparse 

def convert_string_numerical(df, String_typeCols_List): 
    """ 
    Parameters: 
    - df (DataFrame): The PySpark DataFrame to be processed. 
    - String_typeCols_List (list): A list of column names to be cast to double. 
    """ 
    # Cast selected columns to double, leaving others as they are 
    df = df.select([F.col(column).cast('double') 
                        if column in String_typeCols_List else F.col(column) 
                        for column in df.columns]) 
    return df
    
def try_except_wrapper(method, *args, **kwargs): 
    mail_sender = MailSender()
    try: 
        return method(*args, **kwargs) 

    except Exception as e: 
        print(e)
        mail_sender.send(send_from="5gHomeScoreJoinAll@verizon.com",  
                         subject=f"5gHomeScoreJoinAll failed !!!",  
                         text=e) 
        sys.exit()
    
class Join5gTables:
    global hdfs_pa, hdfs_pd

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self,d): 
        self.d = d
        
        self.custline_path = hdfs_pa + "/user/kovvuve/EDW_SPARK/cust_line/"+ self.d
        self.tracfone_path = hdfs_pa + "/user/kovvuve/tracfone/tracfone_v3.csv"
        self.cpe_daily_data_usage_path = hdfs_pd + "/usr/apps/vmas/cpe_daily_data_usage/" + self.d
        self.speedtest_path = hdfs_pd + "/user/ZheS//5g_homeScore/speed_test/" + self.d
        self.fivegTime_path = hdfs_pa + f"/user/derek/hb/result/dt={self.d}/result.csv.gz"
        self.crsp_path = hdfs_pd + "/user/ZheS/5g_homeScore/crsp_result/" + self.d   
        self.oma_path = hdfs_pd + "/user/ZheS/5g_homeScore/oma_result/" + self.d
        self.reset_path = hdfs_pa + f"/fwa_agg/fwa_reset_raw/date={self.d}"
        self.map_path = hdfs_pd + f"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{self.d}/fixed_5g_router_mac_sn_mapping.csv"

        self.cpe_models_to_keep = ["ARC-XCI55AX", "ASK-NCQ1338FA", "WNC-CR200A", "ASK-NCQ1338", "FSNO21VA", "NCQ1338E",'Others'] 
        self.cust_columns = ["cust_id","imei","imsi","mdn_5g","cpe_model_name","PPLAN_DESC","PPLAN_CD"]
        self.fiveg_pplan = ['51219', '27976', '53617', '50044', '50055', '50127', '50128', '67571', '67567', '50129', '67576', '67568', '50116', '50117', '50130', '39425', '39428']
        self.fourg_pplan = ['48390', '48423', '48445', '46799', '46798', '50010', '50011', '67577', '38365','67584', '65655', '65656']

        self.custline_df = try_except_wrapper(self.get_custline_df) 
        self.datausage_df = try_except_wrapper(self.get_datausage_df) 
        self.speedtest_df = try_except_wrapper(self.get_speedtest_df) 
        self.heartbeat_df = try_except_wrapper(self.get_heartbeat_df) 
        self.all_df = try_except_wrapper(self.get_reset_df) 

        #self.custline_df = self.get_custline_df()
        #self.datausage_df = self.get_datausage_df()
        #self.speedtest_df = self.get_speedtest_df()
        #self.heartbeat_df = self.get_heartbeat_df()
        #self.all_df = self.get_reset_df()
        self.df_5gHome = self.apply_plan_conditions( df = self.all_df )

    def get_custline_df(self, custline_path=None, cpe_models_to_keep=None, cust_columns=None, tracfone_path=None):
        if custline_path is None:
            custline_path = self.custline_path
        if cpe_models_to_keep is None:
            cpe_models_to_keep = self.cpe_models_to_keep
        if cust_columns is None:
            cust_columns = self.cust_columns
        if tracfone_path is None:
            tracfone_path = self.tracfone_path
        try:
            df_cust = spark.read.option("recursiveFileLookup", "true").option("header", "true")\
                    .csv(custline_path)\
                    .withColumnRenamed("VZW_IMSI", "imsi")\
                    .withColumnRenamed( "mtn","mdn_5g")\
                    .withColumn("imei", F.expr("substring(IMEI, 1, length(IMEI)-1)"))\
                    .withColumn("cpe_model_name", F.split( F.trim(F.col("device_prod_nm")), " "))\
                    .withColumn("cpe_model_name", F.col("cpe_model_name")[F.size("cpe_model_name") -1])\
                    .withColumn("cpe_model_name", 
                                when(col("cpe_model_name").isin(cpe_models_to_keep), col("cpe_model_name")) 
                                    .otherwise("Others"))\
                    .select(*cust_columns)\
                    .dropDuplicates()
            
            for col_name in df_cust.columns: 
                df_cust = df_cust.withColumnRenamed(col_name, col_name.lower()) 

            df_tracfone = spark.read.option("header","true").csv( tracfone_path)\
                                .withColumnRenamed("DEVICE_ID","imei")\
                                .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))\
                                .withColumnRenamed("IMSI_VZ","imsi")\
                                .withColumnRenamed("MDN","mdn_5g")\
                                .withColumn("cust_id", F.lit("tracfone"))\
                                .withColumn("cpe_model_name", F.lit("tracfone"))\
                                .withColumn("PPLAN_DESC", F.lit("tracfone"))\
                                .withColumn("pplan_cd", F.lit("tracfone"))\
                                .dropDuplicates()

            df_id = df_cust.select(*cust_columns)\
                        .union( df_tracfone.select(*cust_columns) )

            return df_id
        except Exception as e:
            print(e)
            mail_sender.send( send_from ="5gHomeScoreJoinAll@verizon.com", 
                                subject = f"5gHomeScoreJoinAll failed !!! at {d}", 
                                text = e)
    
    def get_datausage_df(self, cpe_daily_data_usage_path=None):
        if cpe_daily_data_usage_path is None:
            cpe_daily_data_usage_path = self.cpe_daily_data_usage_path

        df_datausage = spark.read.option("header","true").csv(cpe_daily_data_usage_path)\
                            .select("cust_id","imei","imsi","mdn_5g","fourg_total_mb","fiveg_total_mb","fiveg_usage_percentage")\
                            .dropDuplicates()
        
        df_datausage = convert_string_numerical(df_datausage,["fourg_total_mb","fiveg_total_mb","fiveg_usage_percentage"])\
                                    .withColumn("data_usage", col("fourg_total_mb")+col("fiveg_total_mb") )\
                                    .withColumn("sqrt_data_usage", F.sqrt( col("fourg_total_mb")+col("fiveg_total_mb") ) )
        return self.custline_df.join(df_datausage.drop("cust_id","imei"),["imsi","mdn_5g"], "left" )

    def get_speedtest_df(self, speedtest_path = None):
        if speedtest_path is None:
            speedtest_path = self.speedtest_path
        df_speedtest = spark.read.parquet( speedtest_path)\
                            .select(F.col("mdn").alias("mdn_5g"),"imei",
                                    F.round(col("downloadresult"), 0).alias("downloadresult"),
                                    F.round(col("uploadresult"), 0).alias("uploadresult"),
                                    F.round(col("latency"), 0).alias("latency"),
                                    "status","progress")\
                            .filter( col("progress") == 100)\
                            .dropDuplicates()\
                            .drop("imei","status","progress")
        return self.datausage_df.join(df_speedtest,"mdn_5g", "left" )
        
    def get_heartbeat_df(self, fivegTime_path=None, crsp_path=None, oma_path=None, tracfone_path=None):
        if fivegTime_path is None:
            fivegTime_path = self.fivegTime_path
        if crsp_path is None:
            crsp_path = self.crsp_path
        if oma_path is None:
            oma_path = self.oma_path

        df_5g4g = spark.read.option("header", "true").csv(fivegTime_path)
        
        df_crsp = spark.read.parquet( crsp_path )\
                        .select("imei","imsi", 
                                "ServicetimePercentage",
                                "switch_count_sum",
                                "percentageReceived",
                                F.round("avg_CQI",2).alias("avg_CQI"),
                                F.round("avg_MemoryPercentFree",2).alias("avg_MemoryPercentFree"),
                                F.round("log_avg_BRSRP",2).alias("log_avg_BRSRP"),
                                F.round("log_avg_4GRSRP",2).alias("log_avg_4GRSRP"),
                                F.round("log_avg_SNR",2).alias("log_avg_SNR"),
                                F.round("log_avg_5GSNR",2).alias("log_avg_5GSNR"),
                                F.round("LTERACHFailurePercentage",2).alias("LTERACHFailurePercentage"),
                                F.round("LTEHandOverFailurePercentage",2).alias("LTEHandOverFailurePercentage"),
                                F.round("NRSCGChangeFailurePercentage",2).alias("NRSCGChangeFailurePercentage"),
                                )\
                        .withColumn("imei", F.expr("substring(imei, 1, length(imei)-1)"))\
                        .join( df_5g4g.select("imsi",
                                            col("hb_total").cast('double'),
                                            col("hb_5g").cast('double'),
                                            (col("percentage").cast('double')*100 ).alias("5g_uptime")), 
                            ["imsi"] )\
                        .dropDuplicates()
        # 4.2. oma_result
        df_oma = spark.read.parquet( oma_path )
        crsp_columns = [col for col in df_crsp.columns if col not in df_oma.columns] 
        for col_name in crsp_columns: 
            df_oma = df_oma.withColumn(col_name, lit(None)) 
        union_df = df_crsp.union(df_oma.select(df_crsp.columns)).dropDuplicates()

        return self.speedtest_df.join(union_df,["imei", "imsi"] ).dropDuplicates()
    
    def get_reset_df(self, reset_path=None, map_path = None):
        if reset_path is None:
            reset_path = self.reset_path
        if map_path is None:
            map_path = self.map_path

        df_reset = spark.read.option("header", "true").csv( reset_path )\
                        .groupby("sn")\
                        .agg( count("*").alias("reset_count") )

        df_map = spark.read.option("header", "true").csv( map_path )\
                        .select("imei", col("serialnumber").alias("sn") )\
                        .dropDuplicates()

        all_df = self.heartbeat_df.join( df_map, ["imei"], "left")\
                                .join( df_reset, "sn", "left" )\
                                .fillna({"reset_count":0})
        return all_df
    
    def apply_plan_conditions(self, df = None,fiveg_pplan=None,fourg_pplan=None):
        if fiveg_pplan is None:
            fiveg_pplan = self.fiveg_pplan
        if fourg_pplan is None:
            fourg_pplan = self.fourg_pplan
        if df is None:
            df = self.all_df
            
        rsrp_conditions = [ (col("PPLAN_CD").isin(fiveg_pplan), col("log_avg_BRSRP")), 
                            (col("PPLAN_CD").isin(fourg_pplan), col("log_avg_4GRSRP")) ]
        snr_conditions = [ (col("PPLAN_CD").isin(fiveg_pplan), col("log_avg_5GSNR")), 
                            (col("PPLAN_CD").isin(fourg_pplan), col("log_avg_SNR")) ]
        
        df = df.withColumn("RSRP",  
                                    when(rsrp_conditions[0][0], rsrp_conditions[0][1]) 
                                    .when(rsrp_conditions[1][0], rsrp_conditions[1][1]) 
                                    .otherwise( col("log_avg_4GRSRP")/2 + col("log_avg_BRSRP")/2  ) 
                                    )\
                        .withColumn("SNR",  
                                    when(snr_conditions[0][0], snr_conditions[0][1]) 
                                    .when(snr_conditions[1][0], snr_conditions[1][1]) 
                                    .otherwise( (col("log_avg_SNR")+col("log_avg_5GSNR"))/2 ) 
                                    )
        return df
    
if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('5g_home_join_all_zhes')\
            .config("spark.sql.adapative.enabled","true")\
            .getOrCreate()

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    backfill_range = 7
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args_date = parser.parse_args().date
    date_list = [( datetime.strptime( args_date, "%Y-%m-%d" )  - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(backfill_range)][::-1]

    for date_str in date_list: 
        try:    
            spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/join_df/"+ date_str)
        except FileNotFoundError:
            df_5gHome = Join5gTables(d = date_str).df_5gHome
            df_5gHome.write.mode("overwrite")\
                    .parquet( hdfs_pd + "/user/ZheS//5g_homeScore/join_df/" + date_str )
