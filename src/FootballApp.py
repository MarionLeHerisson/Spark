import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType


schema = StructType([
    StructField("X2", StringType()),
    StructField("X4", StringType()),
    StructField("X5", StringType()),
    StructField("X6", StringType()),
    StructField("adversaire", StringType()),
    StructField("score_france", StringType()),
    StructField("score_adversaire", StringType()),
    StructField("penalty_france", StringType()),
    StructField("penalty_adversaire", StringType()),
    StructField("date", StringType()),
    StructField("year", StringType()),
    StructField("outcome", StringType()),
    StructField("no", StringType())
])


def main(argv):
    file = "df_matches.csv"
    spark = SparkSession.builder.getOrCreate()
    df_raw_data = spark.read.csv(file, header=True, mode="DROPMALFORMED", schema=schema)

    df_raw_data.show()

    spark.stop()

if __name__ == "__main__":
    main(sys.argv)


