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
    totalMatchs = F.count("*").over(Window.partitionBy(df.adversaire))
    df_adversaires = df.select(df.adversaire)#.groupBy(df.adversaire).show()
    df_stats = df_adversaires.withColumn("match_total", totalMatchs)

# ecrire le resultat dans un fichier parquet nomme stats.parquet

def showStats(df):
    df_with_dom = addDomCol(df)
    calcStats(df_with_dom)
