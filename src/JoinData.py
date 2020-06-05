import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

def readFile():
    spark = SparkSession.builder.getOrCreate()
    df_stats = spark.read.parquet('stats.parquet')
    return df_stats

def joinData(df):
    df_stats = readFile()
    df_stats.show()