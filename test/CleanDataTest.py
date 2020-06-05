import unittest
from pyspark.sql import SparkSession

class CleanDataTest(unittest.TestCase):
    SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
