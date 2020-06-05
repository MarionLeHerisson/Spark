import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType

# nouvelle colonne de type boolean indiquant pour chaque ligne si le match
# a ete joue a domicile (true) ou pas (false)
def is_dom(match):
    return match[0:6] == "France"

def addDomCol(df):
    is_dom_udf = F.udf(is_dom, BooleanType())
    return df.withColumn('match_domicile', is_dom_udf(df.match))

# Pour chaque adversaire de la France, calculez les statistiques suivantes :
# + nombre de point moyen marque par la France par match
# + nombre de point moyen marque par l adversaire par match
# + nombre de match joue total
# + pourcentage de match joue a domicile pour la France
# - nombre de match joue en coupe du monde
# + plus grand nombre de penalite recu par la france dans un match
# + nombre de penalite total recu par la France moins nombre de penalite total recu par l adversaire
def calcStats(df):

    df_stats = (df
        .groupBy("adversaire")
        .agg(
            F.avg(df.score_france).alias("moyenne_france"),
            F.avg(df.score_adversaire).alias("moyenne_adversaire"),
            F.count("*").alias("nombre_total_matchs"),
            (F.sum(df.match_domicile.cast('int')) / F.count(df.adversaire) * 100).alias("pourcentage_domicile"),
#             F.sum().alias("nombre_cdm"),
            F.max(df.penalty_france).alias("max_penalty_france"),
            (F.sum(df.penalty_france) - F.sum(df.penalty_adversaire)).alias("diff_penalty")
        )
    )

    return df_stats

# ecrire le resultat dans un fichier parquet nomme stats.parquet
def writeStats(df):
    df.write.mode('overwrite').parquet('stats.parquet')

def generateStats(df):
    df_with_dom = addDomCol(df)
    df_stats_full = calcStats(df_with_dom)
    writeStats(df_stats_full)
