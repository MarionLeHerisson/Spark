import unittest
import sys
from pyspark.sql import SparkSession
sys.path.append("/home/meh/ESGI/Spark/src")
sys.path.append("/home/meh/ESGI/Spark")

from CleanData import *
# from Statistics import *
# from JoinData import *

class TestSuite(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])

        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == '__main__':
    unittest.main()