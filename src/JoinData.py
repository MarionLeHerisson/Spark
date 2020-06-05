import pyspark.sql.functions as F
from pyspark.sql import SparkSession

class JoinData():

    def readFile(self):
        spark = SparkSession.builder.getOrCreate()
        df_stats = spark.read.parquet('stats.parquet')
        return df_stats

    def joinDfAndFile(self, df_original, df_stats):
        df_stats = df_stats.withColumnRenamed('adversaire', 'adversaireToRemove')

        df_join = df_original.join(
            df_stats,
            df_original.adversaire == df_stats.adversaireToRemove,
            'left'
        )

        df_join = df_join.drop('adversaireToRemove')

        return df_join

    def writeFile(self, df_join):
        df_join.write.partitionBy("date").mode('overwrite').parquet('result.parquet')

    def joinData(self, df_original):
        df_stats = self.readFile()
        df_join = self.joinDfAndFile(df_original, df_stats)
        self.writeFile(df_join)
