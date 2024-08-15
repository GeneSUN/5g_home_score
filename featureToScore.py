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
import argparse 
from functools import reduce

def read_file_by_date(date, file_path_pattern): 
    try:
        file_path = file_path_pattern.format(date) 
        df = spark.read.parquet(file_path) 
        return df 
    except:
        return None

def process_csv_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_file_by_date(date, file_path_pattern), date_range)) 
    df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 
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

def featureToScore(homeScore_df, historical_dist, feature, reverse=False, partitionColumn="cpe_model_name", middle_threshold = 50): 

    # Calculate the percentiles on the historical distribution DataFrame 
    percentiles_df = historical_dist.groupBy(partitionColumn)\
                                .agg( 
                                    F.expr(f'percentile_approx({feature}, 0.5, 10000)').alias(f"{feature}_median"), 
                                    F.expr(f'percentile_approx({feature}, 0.01, 10000)').alias(f"{feature}_lower_5_percentile"), 
                                    F.expr(f'percentile_approx({feature}, 0.99, 10000)').alias(f"{feature}_top_95_percentile") 
                                    ) 

    df_with_percentiles = homeScore_df.join( 
                                            percentiles_df, 
                                            on=partitionColumn 
                                        ) 
    windowSpec = Window.partitionBy(partitionColumn) 

    medianCol = percentile_approx(feature, 0.5, 10000).over(windowSpec) 
    lower5Col = percentile_approx(feature, 0.01, 10000).over(windowSpec) 
    top95Col = percentile_approx(feature, 0.99, 10000).over(windowSpec) 
     
    df_with_percentiles = homeScore_df.withColumn(f"{feature}_median", medianCol)\
                                    .withColumn(f"{feature}_lower_5_percentile", lower5Col)\
                                    .withColumn(f"{feature}_top_95_percentile", top95Col) 
    scale_factor_below = middle_threshold / (col(f"{feature}_median") - col(f"{feature}_lower_5_percentile")) 
    scale_factor_above = (100 - middle_threshold) / (col(f"{feature}_top_95_percentile") - col(f"{feature}_median")) 

    scaled_result = when( 
        col(feature) <= col(f"{feature}_median"), 
        (col(feature) - col(f"{feature}_lower_5_percentile")) * scale_factor_below 
    ).otherwise( 
        (col(feature) - col(f"{feature}_median")) * scale_factor_above + middle_threshold 
    ) 
    if not reverse: 

        df_scaled = df_with_percentiles.withColumn(f"scaled_{feature}", F.round(scaled_result, 2)) 
    else: 
        df_scaled = df_with_percentiles.withColumn(f"scaled_{feature}", F.lit(100.0) - F.round(scaled_result, 2)) 
     
    df_capped = df_scaled.withColumn( 
                                    f"scaled_{feature}", 
                                    when(col(f"scaled_{feature}") < 0, 0) 
                                    .when(col(f"scaled_{feature}") > 100, 100) 
                                    .otherwise(col(f"scaled_{feature}")) 
                                ) 

    return df_capped  

def featureToScoreManual(homeScore_df, feature, threshold_dict,  middle_threshold = 50): 

    @udf(FloatType())
    def get_threshold_value(pplan_cd, index): 
    
        return threshold_dict.get(pplan_cd, [None, None, None])[index] 
                                    
    df_with_percentiles = homeScore_df.withColumn(f"{feature}_lower_5_percentile", when(col("pplan_cd").isin( list( threshold_dict.keys() ) ), get_threshold_value(col("pplan_cd"), lit(0))).otherwise(None))\
                       .withColumn(f"{feature}_median", when(col("pplan_cd").isin(list(threshold_dict.keys())), get_threshold_value(col("pplan_cd"), lit(1))).otherwise(None))\
                       .withColumn(f"{feature}_top_95_percentile", when(col("pplan_cd").isin(list(threshold_dict.keys())), get_threshold_value(col("pplan_cd"), lit(2))).otherwise(None)) 

    scale_factor_below = middle_threshold / (col(f"{feature}_median") - col(f"{feature}_lower_5_percentile")) 
    scale_factor_above = (100 - middle_threshold) / (col(f"{feature}_top_95_percentile") - col(f"{feature}_median")) 

    scaled_result = when( 
        col(feature) <= col(f"{feature}_median"), 
        (col(feature) - col(f"{feature}_lower_5_percentile")) * scale_factor_below 
    ).otherwise( 
        (col(feature) - col(f"{feature}_median")) * scale_factor_above + middle_threshold
    ) 

    df_scaled = df_with_percentiles.withColumn(f"scaled_{feature}", F.round(scaled_result, 2)) 
     
    df_capped = df_scaled.withColumn( 
                                    f"scaled_{feature}", 
                                    when(col(f"scaled_{feature}") < 0, 0) 
                                    .when(col(f"scaled_{feature}") > 100, 100) 
                                    .otherwise(col(f"scaled_{feature}")) 
                                ) 

    return df_capped 

def featureToScoreShfit(homeScore_df, feature, reverse=False, middle_threshold = 50): 

    windowSpec = Window.partitionBy("cpe_model_name") 

    # Calculate the median of the feature for each cpe_model_name 

    medianCol = F.expr(f"percentile_approx({feature}, 0.5, 10000)").over(windowSpec) 
    if not reverse: 
        diff_to_target = middle_threshold - medianCol 
        df_with_scaled_feature = homeScore_df.withColumn(f"{feature}_median", medianCol)\
                                            .withColumn(f"scaled_{feature}", F.round(F.col(feature) + diff_to_target,2) ) 
    else:
        diff_to_target = middle_threshold + medianCol 
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
            .config("spark.ui.port","24041")\
            .enableHiveSupport().getOrCreate()
    
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    cpe_models_to_keep =  ["ARC-XCI55AX", "ASK-NCQ1338FA", "WNC-CR200A", "ASK-NCQ1338", "FSNO21VA", "NCQ1338E","ASK-NCM1100E","ASK-NCM1100",'Others'] 
        
    features = ['imei', 'imsi', 'mdn_5g', 'cust_id', 'cpe_model_name', 'fiveg_usage_percentage', 'downloadresult', 'sn', 'ServicetimePercentage', 'switch_count_sum', 'avg_CQI', 'avg_MemoryPercentFree', 'log_avg_BRSRP', 'log_avg_SNR', 'log_avg_5GSNR', 'LTERACHFailurePercentage', 'LTEHandOverFailurePercentage', 'NRSCGChangeFailurePercentage']
    key_features = ['imei', 'imsi', 'mdn_5g', 'cust_id','sn', 'cpe_model_name']
    scaled_features = ["avg_CQI","log_avg_BRSRP","log_avg_4GRSRP","RSRP","log_avg_SNR","log_avg_5GSNR","SNR","sqrt_data_usage","uploadresult"]
    scaled_features_reverse = ["latency"]
    scaled_features_manual = ["downloadresult",]

    threshold_dict = {
                    "downloadresult": { 

                        '38365': [0.0, 50.0, 75.0], 
                        '50010': [0.0, 50.0, 75.0],  
                        '50011': [0.0, 50.0, 75.0], 
                        '65655': [0.0, 50.0, 75.0], 
                        '65656': [0.0, 50.0, 75.0], 

                        '50127': [0.0, 300.0, 400.0],
                        '50128': [0.0, 300.0, 400.0], 
                        '50129': [0.0, 300.0, 400.0], 
                        '50130': [0.0, 300.0, 400.0], 

                        '39425': [0.0, 1000.0, 1500.0], 
                        '39428': [0.0, 1000.0, 1500.0],

                        '50044': [0.0, 300.0, 400.0], 
                        '50055': [0.0, 300.0, 400.0], 

                        '50116': [0.0, 1000.0, 1500.0], 
                        '50117': [0.0, 1000.0, 1500.0], 

                        '67571': [0.0, 100.0, 150.0], 

                        '67576': [0.0, 300.0, 400.0],  

                        '67567': [0.0, 300.0, 400.0], 

                        '67568': [0.0, 1000.0, 1500.0], 

                        '67577': [0.0, 50.0, 75.0], 
                        '67584': [0.0, 50.0, 75.0], 

                        '51219': [0.0, 100.0, 150.0],  
                        '53617': [0.0, 200.0, 300.0],  

                        '48390': [0.0, 10.0, 15.0], 
                        '48423': [0.0, 25.0, 40.0],  
                        '48445': [0.0, 50.0, 75.0],  

                        '46798': [0.0, 10.0, 15.0],  
                        '46799': [0.0, 25.0, 40.0],  
                    }
                    ,
                    "uploadresult": { 
                        '38365': [0.0, 5.0, 8.0], 
                        '50010': [0.0, 5.0, 8.0], 
                        '50011': [0.0, 5.0, 8.0], 
                        '65655': [0.0, 5.0, 8.0], 
                        '65656': [0.0, 5.0, 8.0], 

                        '50127': [0.0, 20.0, 30.0], 
                        '50128': [0.0, 20.0, 30.0], 
                        '50129': [0.0, 20.0, 30.0], 
                        '50130': [0.0, 20.0, 30.0], 

                        '39425': [0.0, 50.0, 70.0], 
                        '39428': [0.0, 50.0, 70.0],

                        '50044': [0.0, 300.0, 400.0], 
                        '50055': [0.0, 300.0, 400.0], 

                        '50116': [0.0, 1000.0, 1500.0], 
                        '50117': [0.0, 1000.0, 1500.0], 

                        '67571': [0.0, 100.0, 150.0], 

                        '67576': [0.0, 300.0, 400.0],  

                        '67567': [0.0, 300.0, 400.0], 

                        '67568': [0.0, 1000.0, 1500.0], 

                        '67577': [0.0, 50.0, 75.0], 
                        '67584': [0.0, 50.0, 75.0], 

                        '51219': [0.0, 20.0, 30.0],  
                        '53617': [0.0, 20.0, 30.0],  

                        '48390': [0.0, 6.0, 10.0], 
                        '48423': [0.0, 6.0, 10.0],  
                        '48445': [0.0, 6.0, 10.0],  

                        '46798': [0.0, 10.0, 15.0],  
                        '46799': [0.0, 25.0, 40.0],  
                    }
                    
                    } 

    shift_features_reverse = []
    shift_features = ["avg_MemoryPercentFree"]
    
    raw_features_reverse = ["ServicetimePercentage","switch_count_sum","reset_count",'LTERACHFailurePercentage', 'LTEHandOverFailurePercentage', 'NRSCGChangeFailurePercentage'] #ServicetimePercentage means drop service time
    raw_features = ["percentageReceived","fiveg_usage_percentage","5g_uptime"]
        
    day_before = 1
    args_date = date.today() - timedelta(day_before)
    
    backfill_days = 7
    backfill_date_list = [(  args_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(backfill_days)][::-1]
    
    historical_days = 5 # how many days of feature distribution is used for statistical score.
    historical_date_list = [(  args_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(historical_days)][::-1]

    for d in backfill_date_list: 
        try: 
            df_score = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/final_score/"+d)

        except Exception as e: 

            file_path_pattern = hdfs_pd + "/user/ZheS/5g_homeScore/join_df/{}"  
            historical_dist = process_csv_files_for_date_range(historical_date_list, file_path_pattern)
            historical_dist.cache();print( historical_dist.count() )

            df_score = spark.read.parquet(hdfs_pd + "/user/ZheS/5g_homeScore/join_df/"+d)\
                            .withColumn("cpe_model_name", 
                                            when(col("cpe_model_name").isin(cpe_models_to_keep), col("cpe_model_name")) 
                                                .otherwise("Others")) 

            for feature in scaled_features: 
                if feature != "uploadresult":
                    df_score = featureToScore(df_score,historical_dist, feature)
                elif feature == "uploadresult":
                    df_score = featureToScore(df_score,historical_dist, feature, partitionColumn=["cpe_model_name","PPLAN_CD"])

            for feature in scaled_features_reverse: 
                df_score = featureToScore(df_score,historical_dist, feature, reverse=True, partitionColumn=["cpe_model_name","PPLAN_CD"]) 

            for feature in scaled_features_manual: 
                #df_score = featureToScore(df_score, feature, partitionColumn = ["cpe_model_name","PPLAN_CD"] ) 
                df_score = featureToScoreManual(df_score, feature, threshold_dict[feature]) 

            for feature in shift_features_reverse: 
                df_score = featureToScoreShfit(df_score, feature, reverse=True) 
            for feature in shift_features: 
                df_score = featureToScoreShfit(df_score, feature) 

            for feature in raw_features:
                df_score = df_score.withColumn( f"scaled_{feature}", F.round( col(feature),2 ) )

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
            
            df_score = df_score.fillna({"scaled_avg_MemoryPercentFree":50.00})\
                                .withColumn("scaled_fiveg_usage_percentage", col("scaled_fiveg_usage_percentage")/2 )\
                                .withColumn("scaled_5g_uptime", col("scaled_5g_uptime")/2 )
            
            networkSpeedScore_weights = { 
                                    "scaled_uploadresult": 0.3, 
                                    "scaled_downloadresult": 0.4, 
                                    "scaled_latency": 0.3 
                                } 
            networkSignalScore_weights = { 
                                    "scaled_RSRP": 0.35, 
                                    "scaled_avg_CQI": 0.3, 
                                    "scaled_SNR": 0.35, 
                                }  
            networkFailureScore_weights = { 
                                    "scaled_LTERACHFailurePercentage": 0.5, 
                                    "scaled_LTEHandOverFailurePercentage": 0.25, 
                                    "scaled_NRSCGChangeFailurePercentage": 0.25, 
                                }  

            dataScore_weights = { 
                                "scaled_5g_uptime": 0.25, 
                                "scaled_fiveg_usage_percentage": 0.25, 
                                "scaled_sqrt_data_usage": 0.5, 
                                } 
            
            deviceScore_weights = {
                                    "scaled_switch_count_sum": 0.4, 
                                    "scaled_percentageReceived":0.1,
                                    "scaled_reset_count":0.4,
                                    "scaled_avg_MemoryPercentFree":0.1
                                    }
            
            averageScore_weights = {
                                    "dataScore": 0.2, 
                                    "networkSpeedScore":0.2,
                                    "networkSignalScore":0.2,
                                    "networkFailureScore":0.2,
                                    "deviceScore":0.2
                                    }
                        
            score_calculator_networkSpeed = ScoreCalculator(networkSpeedScore_weights) 
            networkSpeed_score_udf = udf(score_calculator_networkSpeed.calculate_score, FloatType()) 

            score_calculator_networkSignal = ScoreCalculator(networkSignalScore_weights) 
            networkSignal_score_udf = udf(score_calculator_networkSignal.calculate_score, FloatType()) 

            score_calculator_networkFailure = ScoreCalculator(networkFailureScore_weights) 
            networkFailure_score_udf = udf(score_calculator_networkFailure.calculate_score, FloatType()) 

            score_calculator_data = ScoreCalculator(dataScore_weights) 
            data_score_udf = udf(score_calculator_data.calculate_score, FloatType()) 

            score_calculator_device = ScoreCalculator(deviceScore_weights) 
            device_score_udf = udf(score_calculator_device.calculate_score, FloatType()) 

            score_calculator_average =ScoreCalculator(averageScore_weights) 
            average_score_udf = udf(score_calculator_average.calculate_score, FloatType()) 

            df_score = df_score.withColumn("dataScore", F.round( data_score_udf(*[col(c) for c in list( dataScore_weights.keys() ) ] ),2) )\
                                .withColumn("networkSpeedScore", F.round( networkSpeed_score_udf(*[col(c) for c in list( networkSpeedScore_weights.keys() ) ] ),2) )\
                                .withColumn("networkSignalScore", F.round( networkSignal_score_udf(*[col(c) for c in list( networkSignalScore_weights.keys() ) ] ),2) )\
                                .withColumn("networkFailureScore", F.round( networkFailure_score_udf(*[col(c) for c in list( networkFailureScore_weights.keys() ) ] ),2) )\
                                .withColumn("deviceScore", F.round( device_score_udf(*[col(c) for c in list( deviceScore_weights.keys() ) ] ),2) )\
                                .withColumn("finalScore", F.round( average_score_udf(*[col(c) for c in list( averageScore_weights.keys() ) ] ),2) )
    
            df_score.dropDuplicates()\
                    .repartition(10)\
                    .write.mode("overwrite")\
                    .parquet( hdfs_pd + "/user/ZheS/5g_homeScore/final_score/" + d )

            """        
            weights_dicts = { 

                "networkSpeedScore_weights": {  
                    "scaled_uploadresult": 0.2,  
                    "scaled_downloadresult": 0.2,  
                    "scaled_latency": 0.2  
                }, 

                "networkSignalScore_weights": {  
                    "scaled_log_avg_4GRSRP": 0.2,  
                    "scaled_log_avg_BRSRP": 0.2,  
                    "scaled_avg_CQI": 0.2,  
                    "scaled_log_avg_SNR": 0.2,  
                    "scaled_log_avg_5GSNR": 0.2  
                }, 

                "networkFailureScore_weights": {  
                    "scaled_LTERACHFailurePercentage": 0.5,  
                    "scaled_LTEHandOverFailurePercentage": 0.25,  
                    "scaled_NRSCGChangeFailurePercentage": 0.25  
                }, 

                "dataScore_weights": {  
                    "scaled_5g_uptime": 0.2,  
                    "scaled_fiveg_usage_percentage": 0.5,  
                    "scaled_sqrt_data_usage": 0.3,  
                }, 

                "deviceScore_weights": { 
                    "scaled_switch_count_sum": 0.25,  
                    "scaled_percentageReceived":0.25, 
                    "scaled_reset_count":0.25, 
                    "scaled_avg_MemoryPercentFree":0.25 
                } 

            } 

            # Apply UDFs to DataFrame 
            for key, weights in weights_dicts.items(): 

                score_calculator = ScoreCalculator(weights) 

                score_udf = udf(score_calculator.calculate_score, FloatType()) 

                cols = [col(c) for c in weights.keys()] 

                df_score = df_score.withColumn(key.replace("_weights", ""), F.round(score_udf(*cols), 2)) 
            """