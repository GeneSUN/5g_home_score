from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,percentile_approx,explode
from pyspark.sql.functions import udf 
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender

def convert_string_numerical(df, String_typeCols_List): 

    df = df.select([F.col(column).cast('double') if column in String_typeCols_List else F.col(column) for column in df.columns]) 

    return df 

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

def featureToScore(homeScore_df, feature, reverse=False): 

    windowSpec = Window.partitionBy("cpe_model_name") 

    medianCol = percentile_approx(feature, 0.5, 10000).over(windowSpec) 
    lower5Col = percentile_approx(feature, 0.01, 10000).over(windowSpec) 
    top95Col = percentile_approx(feature, 0.99, 10000).over(windowSpec) 
     
    df_with_percentiles = homeScore_df.withColumn(f"{feature}_median", medianCol)\
                                    .withColumn(f"{feature}_lower_5_percentile", lower5Col)\
                                    .withColumn(f"{feature}_top_95_percentile", top95Col) 

    if not reverse: 
        scale_factor_below = 60 / (col(f"{feature}_median") - col(f"{feature}_lower_5_percentile")) 
        scale_factor_above = (100 - 60) / (col(f"{feature}_top_95_percentile") - col(f"{feature}_median")) 

        scaled_result = when( 
            col(feature) <= col(f"{feature}_median"), 
            (col(feature) - col(f"{feature}_lower_5_percentile")) * scale_factor_below 
        ).otherwise( 
            (col(feature) - col(f"{feature}_median")) * scale_factor_above + 60 
        ) 

    else: 
        scale_factor_below = 60 / (col(f"{feature}_top_95_percentile") - col(f"{feature}_median")) 
        scale_factor_above = (100 - 60) / (col(f"{feature}_median") - col(f"{feature}_lower_5_percentile")) 
         
        scaled_result = when( 
            col(feature) <= col(f"{feature}_median"), 
            (col(f"{feature}_median") - col(feature)) * scale_factor_below + 60 
        ).otherwise( 
            (col(f"{feature}_top_95_percentile") - col(feature)) * scale_factor_above 
        ) 

    df_scaled = df_with_percentiles.withColumn(f"scaled_{feature}", F.round(scaled_result, 2)) 
     
    df_capped = df_scaled.withColumn( 
                                    f"scaled_{feature}", 
                                    when(col(f"scaled_{feature}") < 0, 0) 
                                    .when(col(f"scaled_{feature}") > 100, 100) 
                                    .otherwise(col(f"scaled_{feature}")) 
                                ) 

    return df_capped 

def featureToScoreShfit(homeScore_df, feature, reverse=False): 

    windowSpec = Window.partitionBy("cpe_model_name") 

    # Calculate the median of the feature for each cpe_model_name 

    medianCol = F.expr(f"percentile_approx({feature}, 0.5, 10000)").over(windowSpec) 
    if not reverse: 
        diff_to_target = 60 - medianCol 
        df_with_scaled_feature = homeScore_df.withColumn(f"{feature}_median", medianCol)\
                                            .withColumn(f"scaled_{feature}", F.round(F.col(feature) + diff_to_target,2) ) 
    else:
        diff_to_target = 60 + medianCol 
        df_with_scaled_feature = homeScore_df.withColumn(f"{feature}_median", medianCol)\
                                            .withColumn(f"scaled_{feature}", F.round( -F.col(feature) + diff_to_target,2) ) 
    # Cap values between 0 and 100 

    df_capped = df_with_scaled_feature.withColumn(  
        f"scaled_{feature}",  
        F.when(F.col(f"scaled_{feature}") < 0, 0) 
        .when(F.col(f"scaled_{feature}") > 100, 100) 
        .otherwise(F.col(f"scaled_{feature}")) 

    ) 
    return df_capped 

if __name__ == "__main__":
    mail_sender = MailSender()
    spark = SparkSession.builder\
            .appName('5gHome_featureToScore')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    
    day_before = 1
    d = ( date.today() - timedelta(day_before) ).strftime("%Y-%m-%d")
    try:
        homeScore_df = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/join_df/"+d)
        homeScore_df = convert_string_numerical(homeScore_df, ["fourg_total_mb","fiveg_total_mb", "fiveg_usage_percentage"])
        cpe_models_to_keep =  ["ARC-XCI55AX", "ASK-NCQ1338FA", "WNC-CR200A", "ASK-NCQ1338", "FSNO21VA", "NCQ1338E",'Others'] 
        homeScore_df = homeScore_df.withColumn("sqrt_data_usage", F.sqrt( col("fourg_total_mb")+col("fiveg_total_mb") ) )\
                                    .withColumn("ServicetimePercentage", col("ServicetimePercentage")*100 )\
                                    .withColumn("cpe_model_name", 
                                            when(col("cpe_model_name").isin(cpe_models_to_keep), col("cpe_model_name")) 
                                                .otherwise("Others")) 
        
        features = ['imei', 'imsi', 'mdn_5g', 'cust_id', 'cpe_model_name', 'fiveg_usage_percentage', 'downloadresult', 'uploadresult', 'latency', 'sn', 'ServicetimePercentage', 'switch_count_sum', 'avg_CQI', 'avg_MemoryPercentFree', 'log_avg_BRSRP', 'log_avg_SNR', 'log_avg_5GSNR', 'LTERACHFailurePercentage', 'LTEHandOverFailurePercentage', 'NRSCGChangeFailurePercentage']
        key_features = ['imei', 'imsi', 'mdn_5g', 'cust_id','sn', 'cpe_model_name']
        scaled_features = ["downloadresult","uploadresult","avg_CQI","log_avg_BRSRP","log_avg_SNR","log_avg_5GSNR","sqrt_data_usage"]
        scaled_features_reverse = ["latency"]
        shift_features_reverse = ["fiveg_usage_percentage"]
        raw_features_reverse = ["ServicetimePercentage","switch_count_sum"]

        df_score = homeScore_df 

        for feature in scaled_features: 
            df_score = featureToScore(df_score, feature) 

        for feature in scaled_features_reverse: 
            df_score = featureToScore(df_score, feature, reverse=True) 

        #for feature in raw_features:
        #    df_score = df_score.withColumn( f"scaled_{feature}", F.round( col(feature),2 ) )

        for feature in raw_features_reverse:

            df_score = df_score.withColumn(f"scaled_{feature}",  
                                    F.round(when(100 - col(feature) < 0, 0) 
                                            .otherwise(100 - col(feature)), 2))\
                                .withColumn(  
                                            f"scaled_{feature}",  
                                            F.when(F.col(f"scaled_{feature}") < 0, 0) 
                                            .when(F.col(f"scaled_{feature}") > 100, 100) 
                                            .otherwise(F.col(f"scaled_{feature}")) 
                                        )# second withColumn seems redundant, i keep here in case it is needed
        for feature in shift_features_reverse: 
            df_score = featureToScoreShfit(df_score, feature, reverse=True) 
        
        networkScore_weights = { 
                                "scaled_log_avg_BRSRP": 0.1, 
                                "scaled_avg_CQI": 0.1, 
                                "scaled_log_avg_SNR": 0.1, 
                                "scaled_log_avg_5GSNR": 0.1, 
                                "scaled_uploadresult": 0.2, 
                                "scaled_downloadresult": 0.2, 
                                "scaled_latency": 0.2 
                            }  
        dataScore_weights = { 
                            "scaled_switch_count_sum": 0.2, 
                            "scaled_fiveg_usage_percentage": 0.5, 
                            "scaled_sqrt_data_usage": 0.3, 
                        } 
        
        score_calculator_network = ScoreCalculator(networkScore_weights) 
        network_score_udf = udf(score_calculator_network.calculate_score, FloatType()) 

        score_calculator_data = ScoreCalculator(dataScore_weights) 
        data_score_udf = udf(score_calculator_data.calculate_score, FloatType()) 

        df_score = df_score.withColumn("dataScore", F.round( data_score_udf(*[col(c) for c in list( dataScore_weights.keys() ) ] ),2) )\
                            .withColumn("networkScore", F.round( network_score_udf(*[col(c) for c in list( networkScore_weights.keys() ) ] ),2) )
        
        df_score.repartition(10)\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/final_score/" + d )
    except Exception as e:
        print(e)
        mail_sender.send( send_from ="featureToScore@verizon.com", 
                            subject = f"featureToScore failed !!! at {d}", 
                            text = e)
