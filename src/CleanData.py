from pyspark.sql import SparkSession

# Lire le fichier csv dans une dataframe
def readCsv():
    file = "df_matches.csv"
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(file, header=True, sep=",").cache()

# Renommer les colonnes :
# - X4 : match
# - X6 : competition
def renameColumns(df):
    return df.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")

# Selectionner les colonnes :
# - match
# - competition
# - adversaire
# - score_france
# - score_adversaire
# - penalty_france
# - penalty_adversaire
# - date
def selectColumns(df):
    return df.select(df.match, df.competition, df.adversaire, df.score_france.cast('int'), df.score_adversaire.cast('int'), df.penalty_france.cast('int'), df.penalty_adversaire.cast('int'), df.date)

# Completer les valeurs nulles dans les colonnes penalty par des 0
def fillWithZeros(df):
    return df.na.fill(0)

# Filtrer et garder uniquement les matchs datant de mars 1980 a aujourd hui
def keepOnlyFromEightees(df):
    return df.filter(df.date >= '1980-03-01')

def getCleanData():
    df_raw_data = readCsv()
    df_renamed_data = renameColumns(df_raw_data)
    df_selected_columns = selectColumns(df_renamed_data)
    df_without_null = fillWithZeros(df_selected_columns)
    df_clean_data = keepOnlyFromEightees(df_without_null)

    return df_clean_data
