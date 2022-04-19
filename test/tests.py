import unittest
import chispa
from pyspark.sql import SparkSession
from pyspark.sql import utils
from  src import main
import os

class dataReadTests(unittest.TestCase):

    def setUp(self):
        self.session = SparkSession.builder\
          .appName("test_task")\
          .getOrCreate()
        self.cwd = os.getcwd()
          
    def test_fail_if_file_does_not_exist(self):
        non_existent_file_name = self.cwd + '/my_bogus_file.csv'
        with self.assertRaises(utils.AnalysisException):
            main.read_file(non_existent_file_name, self.session)