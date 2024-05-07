#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.3.6 
export SPARK_HOME=/usr/apps/vmas/spark 
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3  

echo "RES: Starting===" 
echo $(date) 

# Prompt user for input 
read -p "Enter the date (YYYY-MM-DD): " date_input 

# Check if date_input is empty 

if [ -z "$date_input" ]; then 
    # If date_input is empty, call the Python script without specifying the date 
    /usr/apps/vmas/spark/bin/spark-submit \
    --master spark://njbbvmaspd11.nss.vzwnet.com:7077 \
    --conf "spark.sql.session.timeZone=UTC" \ 
    --conf "spark.driver.maxResultSize=16g" \
    --conf "spark.dynamicAllocation.enabled=false" \
    --num-executors 50 \
    --executor-cores 2 \
    --total-executor-cores 100 \
    --executor-memory 10g \
    --driver-memory 64g \
    /usr/apps/vmas/scripts/ZS/5g_home_score/featureToScore.py \
    > /usr/apps/vmas/scripts/ZS/5g_home_score/featureToScore.log 

else 

    # If date_input is not empty, call the Python script with the specified date 

    /usr/apps/vmas/spark/bin/spark-submit \
    --master spark://njbbvmaspd11.nss.vzwnet.com:7077 \
    --conf "spark.sql.session.timeZone=UTC" \
    --conf "spark.driver.maxResultSize=16g" \
    --conf "spark.dynamicAllocation.enabled=false" \
    --num-executors 50 \
    --executor-cores 2 \
    --total-executor-cores 100 \
    --executor-memory 10g \
    --driver-memory 64g \
    /usr/apps/vmas/scripts/ZS/5g_home_score/featureToScore.py \
    --date "$date_input" \
    > /usr/apps/vmas/scripts/ZS/5g_home_score/featureToScore.log 

fi 