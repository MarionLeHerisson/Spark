from pyspark.sql import SparkSession

class CleanData():

    def readCsv(self, filename):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(filename, header=True, sep=",").cache()

    def renameColumns(self, df):
        return df.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")

    def selectColumns(self, df):
        return df.select(df.match, df.competition, df.adversaire, df.score_france.cast('int'), df.score_adversaire.cast('int'), df.penalty_france.cast('int'), df.penalty_adversaire.cast('int'), df.date)

    def fillWithZeros(self, df):
        return df.na.fill(0)

    def keepOnlyFromEightees(self, df):
        return df.filter(df.date >= '1980-03-01')

    def getCleanData(self, filename):
        df_raw_data = self.readCsv(filename)
        df_renamed_data = self.renameColumns(df_raw_data)
        df_selected_columns = self.selectColumns(df_renamed_data)
        df_without_null = self.fillWithZeros(df_selected_columns)
        df_clean_data = self.keepOnlyFromEightees(df_without_null)

        return df_clean_data
