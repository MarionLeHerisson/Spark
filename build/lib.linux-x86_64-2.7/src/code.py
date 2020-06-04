import sys
from pyspark.sql import SparkSession

def main(argv):
    print(argv[1])
    
    dataFile = "df_matches.csv"
    spark = SparkSession.builder.appName("FootballApp").getOrCreate()
    logData = spark.read.text(dataFile).cache()

    numAs = logData.filter(logData.value.contains('a')).count()
    numBs = logData.filter(logData.value.contains('b')).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

    spark.stop()

if __name__ == "__main__":
    main(sys.argv)


