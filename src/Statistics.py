import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

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
    window = Window.partitionBy(df.adversaire)

    colAvgPointsFrance = F.avg(df.score_france).over(window)
    colAvgPointsAdversaire = F.avg(df.score_adversaire).over(window)
    colTotalMatchs = F.count("*").over(window)
    colPercentDom = F.sum(df.match_domicile.cast('int')).over(window) / F.count(df.adversaire).over(window) * 100
#   colNbWorldCup = F.sum()
    colMaxPenalty = F.max(df.penalty_france).over(window)
    colDiffPenalty = F.sum(df.penalty_france).over(window) - F.sum(df.penalty_adversaire).over(window)

    df_stats = df.withColumn("moyenne_france", colAvgPointsFrance)
    df_stats = df_stats.withColumn("moyenne_adversaire", colAvgPointsAdversaire)
    df_stats = df_stats.withColumn("nombre_total_matchs", colTotalMatchs)
    df_stats = df_stats.withColumn("pourcentage_domicile", colPercentDom)
#     df_stats = df_stats.withColumn("nombre_cdm", colNbWorldCup)
    df_stats = df_stats.withColumn("max_penalty_france", colMaxPenalty)
    df_stats = df_stats.withColumn("diff_penalty", colDiffPenalty)

    return df_stats

# ecrire le resultat dans un fichier parquet nomme stats.parquet
def writeStats(df):
    df.write.mode('overwrite').parquet('stats.parquet')

def showStats(df):
    df_with_dom = addDomCol(df)
    df_stats_full = calcStats(df_with_dom)
    df_stats_full.show()
    writeStats(df_stats_full)
