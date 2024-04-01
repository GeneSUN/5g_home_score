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

@udf(returnType=FloatType())
def calculate_DataScore(switch_count_sum, fiveg_usage_percentage, sqrt_data_usage): 

    weight_switch_count_sum = 0.2 
    
    weight_fiveg_usage_percentage = 0.5 
    
    weight_sqrt_data_usage = 0.3 

    if switch_count_sum is None and fiveg_usage_percentage is None and sqrt_data_usage is None: 
        return None 
    else: 
        total_weight = 0 
        score = 0 
        if switch_count_sum is not None: 
            score += weight_switch_count_sum * switch_count_sum 
            total_weight += weight_switch_count_sum 
        if fiveg_usage_percentage is not None: 
            score += weight_fiveg_usage_percentage * fiveg_usage_percentage 
            total_weight += weight_fiveg_usage_percentage 
        if sqrt_data_usage is not None: 
            score += weight_sqrt_data_usage * sqrt_data_usage 
            total_weight += weight_sqrt_data_usage 
        return score / total_weight 
        
@udf(returnType=FloatType())
def calculate_NetworkScore( log_avg_BRSRP, avg_CQI, log_avg_SNR, log_avg_5GSNR, uploadresult, downloadresult, latency): 

    weight_log_avg_BRSRP = 0.1 
    weight_avg_CQI = 0.1
    weight_log_avg_SNR = 0.1
    weight_log_avg_5GSNR = 0.1 
    weight_uploadresult = 0.2
    weight_downloadresult = 0.2 
    weight_latency = 0.2
        
    total_weight = 0 
    score = 0 
    if log_avg_BRSRP is None and avg_CQI is None and log_avg_SNR is None and log_avg_5GSNR is None and uploadresult is None and downloadresult is None and latency is None: 
        return None 
    else: 
        total_weight = 0 
        score = 0 

        if log_avg_BRSRP is not None:  
            score += weight_log_avg_BRSRP * float(log_avg_BRSRP)  
            total_weight += weight_log_avg_BRSRP  
        if avg_CQI is not None:  
            score += weight_avg_CQI * float(avg_CQI)  
            total_weight += weight_avg_CQI  
        if log_avg_SNR is not None:  
            score += weight_log_avg_SNR * float(log_avg_SNR)  
            total_weight += weight_log_avg_SNR  
        if log_avg_5GSNR is not None:  
            score += weight_log_avg_5GSNR * float(log_avg_5GSNR)  
            total_weight += weight_log_avg_5GSNR  
        if uploadresult is not None:  
            score += weight_uploadresult * float(uploadresult)  
            total_weight += weight_uploadresult  
        if downloadresult is not None:  
            score += weight_downloadresult * float(downloadresult)  
            total_weight += weight_downloadresult  
        if latency is not None:  
            score += weight_latency * float(latency)  
            total_weight += weight_latency  
        return score / total_weight  

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
            #df_score = df_score.withColumn( f"scaled_{feature}", F.round( 100 - col(feature) ) )
            df_score = df_score.withColumn(f"scaled_{feature}",  
                                    F.round(when(100 - col(feature) < 0, 0) 
                                            .otherwise(100 - col(feature)), 2))\
                                .withColumn(  
                                            f"scaled_{feature}",  
                                            F.when(F.col(f"scaled_{feature}") < 0, 0) 
                                            .when(F.col(f"scaled_{feature}") > 100, 100) 
                                            .otherwise(F.col(f"scaled_{feature}")) 
                                        )
        for feature in shift_features_reverse: 
            df_score = featureToScoreShfit(df_score, feature, reverse=True) 
        

        df_score = df_score.withColumn("dataScore", F.round( calculate_DataScore("scaled_switch_count_sum", "scaled_fiveg_usage_percentage", "scaled_sqrt_data_usage"),2)  )
        df_score = df_score.withColumn("NetworkScore", F.round( calculate_NetworkScore("scaled_log_avg_BRSRP", "scaled_avg_CQI", "scaled_log_avg_SNR", "scaled_log_avg_5GSNR", "scaled_uploadresult", "scaled_downloadresult", "scaled_latency"),2) )

        df_score.repartition(10)\
                .write.mode("overwrite")\
                .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/final_score/" + d )
    except Exception as e:
        print(e)
        mail_sender.send( send_from ="featureToScore@verizon.com", 
                            subject = f"featureToScore failed !!! at {d}", 
                            text = e)
