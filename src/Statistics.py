import pyspark.sql.functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql import SparkSession

# nouvelle colonne de type boolean indiquant pour chaque ligne si le match
# a ete joue a domicile (true) ou pas (false)
def is_dom(match):
    return match[0:6] == "France"

def addDomCol(df):
    is_dom_udf = F.udf(is_dom, BooleanType())
    df_with_dom = df.withColumn('match_domicile', is_dom_udf(df.match))
    return df_with_dom

# Pour chaque adversaire de la France, calculez les statistiques suivantes :
# - nombre de point moyen marque par la France par match
# - nombre de point moyen marque par l adversaire par match
# - nombre de match joue total
# - pourcentage de match joue a domicile pour la France
# - nombre de match joue en coupe du monde
# - plus grand nombre de penalite recu par la france dans un match
# - nombre de penalite total recu par la France moins nombre de penalite total recu par l adversaire

# ecrire le resultat dans un fichier parquet nomme stats.parquet

def showStats(df):
    df_with_dom = addDomCol(df)
    df_with_dom.show()
