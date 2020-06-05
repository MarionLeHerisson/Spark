import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType

class Statistics():

    def isDom(self, match):
        return match[0:6] == "France"

    def isWorldCup(self, competition):
        return competition[0:14] == "Coupe du monde"

    def addDomCol(self, df):
        is_dom_udf = F.udf(self.isDom, BooleanType())
        return df.withColumn('match_domicile', is_dom_udf(df.match))

    def calcStats(self, df):
        is_world_cup_udf = F.udf(self.isWorldCup, BooleanType())

        df_stats = (df
            .groupBy("adversaire")
            .agg(
                F.avg(df.score_france).alias("moyenne_france"),
                F.avg(df.score_adversaire).alias("moyenne_adversaire"),
                F.count("*").alias("nombre_total_matchs"),
                (F.sum(df.match_domicile.cast('int')) / F.count(df.adversaire) * 100).alias("pourcentage_domicile"),
                F.sum(is_world_cup_udf(df.competition).cast('int')).alias("nombre_cdm"),
                F.max(df.penalty_france).alias("max_penalty_france"),
                (F.sum(df.penalty_france) - F.sum(df.penalty_adversaire)).alias("diff_penalty")
            )
        )

        return df_stats

    def writeStats(self, df):
        df.write.mode('overwrite').parquet('stats.parquet')

    def generateStats(self, df):
        df_with_dom = self.addDomCol(df)
        df_stats_full = self.calcStats(df_with_dom)
        self.writeStats(df_stats_full)
