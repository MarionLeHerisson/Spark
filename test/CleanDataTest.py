import unittest

class CleanDataTest(unittest.TestCase):
    SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

}