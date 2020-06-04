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
# - nombre de point moyen marque par la France par match
# - nombre de point moyen marque par l adversaire par match
# - nombre de match joue total
# - pourcentage de match joue a domicile pour la France
# - nombre de match joue en coupe du monde
# - plus grand nombre de penalite recu par la france dans un match
# - nombre de penalite total recu par la France moins nombre de penalite total recu par l adversaire
def calcStats(df):
    window = Window.partitionBy(df.adversaire)

    avgPointsFrance = F.avg(df.score_france).over(window)
    avgPointsAdversaire = F.avg(df.score_adversaire).over(window)
    totalMatchs = F.count("*").over(window)

    df_stats = df.withColumn("moyenne_france", avgPointsFrance)
    df_stats = df_stats.withColumn("moyenne_adversaire", avgPointsAdversaire)
    df_stats = df_stats.withColumn("nombre_total_matchs", totalMatchs)

    df_stats.show()

# ecrire le resultat dans un fichier parquet nomme stats.parquet
def writeStats():
    return null

def showStats(df):
    df_with_dom = addDomCol(df)
    calcStats(df_with_dom)
