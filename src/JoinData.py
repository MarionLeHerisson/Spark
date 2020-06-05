import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

class JoinData():

    def readFile(self):
        spark = SparkSession.builder.getOrCreate()
        df_stats = spark.read.parquet('stats.parquet')
        return df_stats

    def joinData(self, df):
        df_stats = self.readFile()
        df_stats.show()