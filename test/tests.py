import unittest
import chispa
from pyspark.sql import SparkSession
from pyspark.sql import utils, dataframe
from  src import main
import os

class dataReadTests(unittest.TestCase):

    def setUp(self):
        self.session = SparkSession.builder\
          .master('local')\
          .appName('test_task')\
          .getOrCreate()
        self.sample_file_name = 'test/test_dataset.csv'
          
    def test_fail_if_file_does_not_exist(self):
        non_existent_file_name = '/my_bogus_file.csv'
        with self.assertRaises(utils.AnalysisException):
            main.read_file(non_existent_file_name, self.session)
            
    def test_return_fd_if_file_exists(self):
        my_file = self.sample_file_name
        the_file = main.read_file(my_file, self.session)
        self.assertIsInstance(the_file, dataframe.DataFrame)
     
     
class dataFilterTests(unittest.TestCase):
    
    def setUP(self):
        self.session = SparkSession.builder\
          .master('local')\
          .appName('test_task')\
          .getOrCreate()
        self.sample_file_name = 'test/test_dataset.csv'