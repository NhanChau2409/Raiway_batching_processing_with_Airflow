from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

import requests as re
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StructType, MapType

# To succes in pulling API from Airflow
import os
os.environ["no_proxy"]="*"

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt



@task()
def etl_pipeline():
    
    PATH_TO_JAR_POSTGRE = '/Users/nhanchau/Desktop/postgresql-42.5.1.jar'
    
    spark = (SparkSession
        .builder
        .master('local[*]')
        .appName('Railway Traffic')
        .config('spark.jars', PATH_TO_JAR_POSTGRE)
        .getOrCreate())
    
    # -------------------------------EXTRACT-------------------------------
    res_dict = {'trains_res' : re.get('https://rata.digitraffic.fi/api/v1/metadata/train-types').text,
                'category_res' : re.get('https://rata.digitraffic.fi/api/v1/metadata/train-categories').text,
                'station_res' : re.get('https://rata.digitraffic.fi/api/v1/metadata/stations').text,
                'operator_res' : re.get('https://rata.digitraffic.fi/api/v1/metadata/operators').text}
    
    df_dict = {k: spark.createDataFrame(json.loads(v)) for (k,v) in res_dict.items()}
    
    # -------------------------------TRANSFORM-------------------------------
    def flaten_df(df: DataFrame, sep = '_'):
        
        df = df.select([F.col(col).alias(col.lower()) for col in df.columns])

        complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
            or type(field.dataType) == StructType
            or type(field.dataType) == MapType
        ]
        )

        while len(complex_fields) != 0: 
            col_name = list(complex_fields.keys())[0]
            
            if type(complex_fields[col_name]) == StructType:
                expanded = [
                    F.col(col_name + "." + k).alias(col_name + sep + k)
                    for k in [n.name for n in complex_fields[col_name]]
                ]
                df = df.select("*", *expanded).drop(col_name)

            elif type(complex_fields[col_name]) == ArrayType:
                df = df.withColumn(col_name, F.explode_outer(col_name))

            elif type(complex_fields[col_name]) == MapType:
                keys = (df.select(F.explode(col_name))
                        .select("key")
                        .distinct()
                        .rdd.flatMap(lambda x: x)
                        .collect())
                
                df = df.select('*', *[F.col(col_name).getItem(k).alias(col_name + sep + k) for k in keys]).drop(col_name)


            complex_fields = dict(
                [
                    (field.name, field.dataType)
                    for field in df.schema.fields
                    if type(field.dataType) == ArrayType
                    or type(field.dataType) == StructType
                    or type(field.dataType) == MapType
                ]
            )
        return df
    
    
    df_flaten_dict = {k: flaten_df(v) for (k,v) in df_dict.items()}
    # -------------------------------LOAD-------------------------------    
    mode = "overwrite"
    url = "jdbc:postgresql://localhost:5432/airflow_db"
    properties = {"user": "airflow_user","password": "airflow_pass","driver": "org.postgresql.Driver"}

    for k, v in df_flaten_dict.items():
        (v
         .write
         .jdbc(url=url, table=k, mode=mode, properties=properties))  
        
    spark.stop()      

@dag(dag_id = 'railway_v02',
     default_args = {'owner': 'Nhan_Chau',
                     'retries': 5,
                     'retry_delay': timedelta(minutes=5)},
     description = 'Batching processing with Finland railway data and visualize data',
     start_date = datetime(2022, 12, 1),
     schedule_interval = '@daily'
     ) 
def etl():
    etl_pipeline()
    
etl()