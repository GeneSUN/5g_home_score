from pyspark.sql import SparkSession 
import argparse 
import requests 
import pandas as pd 
import json
from datetime import timedelta, date , datetime
import argparse 
import requests 
import pandas as pd 
import json
import psycopg2
import sys
import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from ftplib import FTP
import pandas as pd

from hdfs import InsecureClient 
import os
import re 

class ClassDailyCheckBase:
    global neglect_days
    neglect_days = ["2023-11-23", "2023-11-24","2023-11-27", 
                    "2023-12-25", "2023-12-26","2023-12-28", 
                    "2024-01-01",  "2024-01-02",
                    "2024-01-15","2024-01-16","2024-05-27",
                    "2024-07-04", "2024-07-03", "2024-07-02", "2024-07-01"]
    def __init__(self, name, expect_delay):
        self.name = name
        self.expect_delay = expect_delay
        self.expect_date = date.today() - timedelta( days = self.expect_delay )

class ClassDailyCheckHdfs(ClassDailyCheckBase): 

    def __init__(self,  hdfs_host, hdfs_port, hdfs_folder_path, file_name_pattern, *args, **kwargs): 
        super().__init__(*args, **kwargs) 
        self.hdfs_host = hdfs_host 
        self.hdfs_port = hdfs_port 
        self.hdfs_folder_path = hdfs_folder_path 
        self.file_name_pattern = file_name_pattern 
        self.hdfs_latest_file = None 
        self.hdfs_latest_date = None 
        self.hdfs_delayed_days = None 
        self.find_latest_hdfs_file() 
        self.set_hdfs_date() 
        self.hdfs_miss_days = self.find_all_empty()
        
    def find_latest_hdfs_file(self): 
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        latest_file = None 
        latest_date = None 
        date_pattern = re.compile(self.file_name_pattern) 

        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d') 
                if latest_date is None or folder_name_date > latest_date: 
                    latest_file = file_name 
                    latest_date = folder_name_date
                    
        if latest_file: 
            self.hdfs_latest_file = latest_file 
            self.hdfs_latest_date = latest_date 
        else: 
            self.hdfs_latest_file = None 
            self.hdfs_latest_date = None
    
    def set_hdfs_date(self): 
        self.hdfs_delayed_days = (self.expect_date -self.hdfs_latest_date.date()).days 
    
    def find_hdfs_file(self, search_date = None): 
        
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        date_pattern = re.compile(self.file_name_pattern) 
    
        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
    
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if folder_name_date == search_date: 
                    return True

    def find_all_empty(self, expect_date = None, time_window = None):
        if time_window is None:
            time_window = time_range
        if expect_date == None:
            expect_date = self.expect_date
        start_date = expect_date

        end_date = start_date - timedelta(days = time_window)
        cur_date = start_date
        miss_days = []
        while cur_date >= end_date:
            cur_date -= timedelta(days = 1)

            if self.find_hdfs_file(cur_date):
                #print(f"{cur_date} existed")
                pass
            else:
                miss_days.append(f"{cur_date}")
        
        if self.name[:9] == 'snap_data':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in miss_days] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days]
            return filtered_dates
        else:
            return miss_days
        return miss_days

class ClassDailyCheckDruid(ClassDailyCheckHdfs): 

    def __init__(self, init_payload,miss_payload, API_ENDPOINT="http://njbbvmaspd6.nss.vzwnet.com:8082/druid/v2/sql", *args, **kwargs): 

        super().__init__(*args, **kwargs)
        self.payload = init_payload
        self.druid_latest_date = None
        self.druid_delayed_days = None
        self.API_ENDPOINT = API_ENDPOINT
        self.set_druid_date()
        self.miss_payload = miss_payload
        
        self.druid_miss_days = self.find_miss_druid()
        
    def set_druid_date(self):
        r = self.query_druid(self.payload).json()
        self.druid_latest_date = datetime.strptime(r[0]['current_date_val'], "%Y-%m-%d").date() 
        self.druid_delayed_days = (self.expect_date -self.druid_latest_date).days
         
    def query_druid(self, payload = None, ): 

        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}   
        payload = json.dumps(payload) 
        response = requests.post(self.API_ENDPOINT, data=payload, headers=headers) 
        return response
    
    def find_miss_druid(self, p = None, task_name = None , time_window = None):
        if p is None:
            p = self.miss_payload
        if task_name is None:
            task_name = self.name
        if time_window is None:
            time_window = time_range
        current_date = datetime.now().date()  - timedelta(days= int(expected_delay[task_name]) )
        #current_date = datetime.now().date()
        target_date_list = [ datetime.strftime( (current_date - timedelta(days=i)), "%Y-%m-%d") for i in range(time_window)]
        
        
        dict_list = self.query_druid( p ).json()
        exist_date_list = [item["existed_date"] for item in dict_list]
    
        missing_dates = [date for date in target_date_list if date not in exist_date_list ]

        
        if task_name[:13] == 'snap_data_pre':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in missing_dates] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days ]
            return filtered_dates
        else:
            return missing_dates


def send_mail(send_from, send_to, subject, cc,df_list, files=None, server='vzsmtp.verizon.com' ):
    assert isinstance(send_to, list)
    assert isinstance(cc, list)
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = COMMASPACE.join(cc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    # Convert DataFrames to HTML tables and add <br> tags 
    html_content = '<br><br>'.join(df.to_html() for df in df_list) 
    msg.attach(MIMEText(html_content, 'html')) 
    
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to + cc, msg.as_string())
    smtp.close

def get_hdfs_missing(hdfs_location):
    hdfs_list = list( hdfs_location.keys() )

    Hdfs_dict = {}
    for Daily_Task_name in hdfs_list:
        try:
            Hdfs_dict[Daily_Task_name] =ClassDailyCheckHdfs(
                                                    hdfs_location[Daily_Task_name]["hdfs_host"], 
                                                    hdfs_location[Daily_Task_name]["hdfs_port"], 
                                                    hdfs_location[Daily_Task_name]["hdfs_folder_path"], 
                                                    hdfs_location[Daily_Task_name]["file_name_pattern"],
                                                    Daily_Task_name, 
                                                    int(expected_delay[Daily_Task_name])
                                                    )
        except:
            print(f"no hdfs of {Daily_Task_name}")

    data_hdfs = [vars( ins ) for ins in list( Hdfs_dict.values() ) ]
    df_hdfs = pd.DataFrame(data_hdfs)[["name","hdfs_latest_date","hdfs_delayed_days","hdfs_miss_days"]]
    return df_hdfs

def get_druid_missing(druid_hdfs_location, payload, miss_date_payload):
    druid_dict = {}
    #for daily_task in payload.keys():
    for daily_task in druid_hdfs_location.keys():
        try:
            if daily_task == "cpe_final_score_v3":
                druid_dict[daily_task] = ClassDailyCheckDruid( 
                                init_payload = payload[daily_task], 
                                miss_payload = miss_date_payload[daily_task],
                                API_ENDPOINT = "http://njbbepapa6.nss.vzwnet.com:8082/druid/v2/sql",
                                hdfs_host = druid_hdfs_location[daily_task]["hdfs_host"], 
                                hdfs_port = druid_hdfs_location[daily_task]["hdfs_port"], 
                                hdfs_folder_path = druid_hdfs_location[daily_task]["hdfs_folder_path"], 
                                file_name_pattern = druid_hdfs_location[daily_task]["file_name_pattern"],
                                name = daily_task, 
                                expect_delay = int(expected_delay[daily_task]) )
            else:
                druid_dict[daily_task] = ClassDailyCheckDruid( 
                                                init_payload = payload[daily_task], 
                                                miss_payload = miss_date_payload[daily_task],
                                                hdfs_host = druid_hdfs_location[daily_task]["hdfs_host"], 
                                                hdfs_port = druid_hdfs_location[daily_task]["hdfs_port"], 
                                                hdfs_folder_path = druid_hdfs_location[daily_task]["hdfs_folder_path"], 
                                                file_name_pattern = druid_hdfs_location[daily_task]["file_name_pattern"],
                                                name = daily_task, 
                                                expect_delay = int(expected_delay[daily_task]) )
        except Exception as e:
            print(e)
    data_druid = [vars( ins ) for ins in list( druid_dict.values() ) ]
    df_druid = pd.DataFrame(data_druid)[["name","druid_latest_date","druid_delayed_days","druid_miss_days","hdfs_latest_date","hdfs_delayed_days","hdfs_miss_days"]]
    return df_druid

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Daily SNA data availability checking')\
                        .config("spark.ui.port","24040")\
                        .getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")

    # 1. expected_delay is global variable used in Class
    expected_delay = {  
                    "cust_line":1,
                    "cpe_daily_data_usage":1,
                    "speed_test":1,
                    "fivegTime":1,
                    "crsp_result":1,
                    "oma_result":1,
                    "reset":1,
                    "map":1,
                    "join_df":1,
                    "final_score":1,
                    "cpe_final_score_v3":1
                    }
    time_range = time_window = 10

    hdfs_location = { 
                'cust_line': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/kovvuve/EDW_SPARK/cust_line/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                "cpe_daily_data_usage": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/usr/apps/vmas/cpe_daily_data_usage/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "speed_test": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '//user/ZheS//5g_homeScore/speed_test/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                'fivegTime': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/derek/hb/result/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                "crsp_result": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '//user/ZheS//5g_homeScore/crsp_result/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "oma_result": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '//user/ZheS//5g_homeScore/oma_result/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "reset": {
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/fwa_agg/fwa_reset_raw/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "map": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "join_df": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/5g_homeScore/join_df/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "final_score": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/5g_homeScore/final_score/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
        }
    df_hdfs = get_hdfs_missing(hdfs_location)

    druid_hdfs_location = { 
                "cpe_final_score_v3": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/5g_homeScore/final_score/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
        }

    tables = list(druid_hdfs_location.keys())
    # check if missing for today ------------------------------------------------------------
    payload_base_query = """ 
        SELECT SUBSTR(CAST(__time AS VARCHAR), 1, 10) as current_date_val, 
            COUNT(*) as row_count  
        FROM {table_name}  
        GROUP BY SUBSTR(CAST(__time AS VARCHAR), 1, 10)  
        ORDER BY current_date_val DESC  
        LIMIT 1 
    """ 
    payload = {table: {"query": payload_base_query.format(table_name=table)} for table in tables} 
    # check missing days ------------------------------------------------------------------------
    query_template = """ 
        SELECT DISTINCT SUBSTR(CAST(__time AS VARCHAR), 1, 10) as existed_date  
        FROM {table_name}  
        ORDER BY existed_date DESC  
        LIMIT {time_window} 
    """ 
    miss_date_payload = {table: {"query": query_template.format(table_name=table, time_window=time_range)} for table in tables} 
    try:
        df_druid = get_druid_missing(druid_hdfs_location, payload, miss_date_payload)
    except Exception as e:
        print(e)

#-----------------------------------------------------------------------------------------

    df_list = [ df_druid, df_hdfs,]
    
    send_mail(  'sassupport@verizon.com', 
            ['zhe.sun@verizonwireless.com',], 
            f"cpe_final_score_v3 availability checking",
            [], 
            df_list, 
            files=None, 
            server='vzsmtp.verizon.com' )
