import pytest
import unittest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql import utils, dataframe, functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src import main

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
     
     
@pytest.fixture()
def session():
    my_session = SparkSession.builder\
      .master('local')\
      .appName('chispa')\
      .getOrCreate()
    return my_session


@pytest.fixture()
def schema():
    my_schema = StructType([
                        StructField('id', IntegerType(), nullable=False),
                        StructField('first_name', StringType(), nullable=False),
                        StructField('last name', StringType(), nullable=False),
                        ])
    return my_schema


@pytest.fixture()
def data():
    return [
            (1, 'Hosea', 'Odonnell'),
            (2, 'Murray', 'Weber'),
            (3, 'Emory', 'Giles'),
            (4, 'Devin', 'Ayala'),
            (5, 'Rebekah', 'Rosario'),
            (6, 'Tracy', 'Gardner'),
            (7, 'Hosea', 'Blackwell'),
            (8, 'Madeline', 'Black'),
            (9, 'Jim', 'Delacruz'),
            (10, 'Abigail', 'Giles')
        ]


@pytest.fixture()
def df(session, schema, data):
    return session.createDataFrame(data=data, schema=schema)


def test_filtering_by_collection_leaves_needed_rows(session, schema, df):
    remaining_data = [
            (1, 'Hosea', 'Odonnell'),
            (5, 'Rebekah', 'Rosario'),
            (7, 'Hosea', 'Blackwell')
            ]
    remaining_df = session.createDataFrame(data=remaining_data, 
                                                schema=schema)
    filters = ['Hosea', 'Rebekah']
    filtered = main.filter_df(df, 'first_name', filters)
    assert_df_equality(remaining_df, filtered)
    
    
def test_filtering_by_string_leaves_needed_rows(session, schema, df):
    remaining_data = [
            (1, 'Hosea', 'Odonnell'),
            (7, 'Hosea', 'Blackwell')
            ]
    remaining_df = session.createDataFrame(data=remaining_data, 
                                                schema=schema)
    filters = 'Hosea'
    filtered = main.filter_df(df, 'first_name', filters)
    assert_df_equality(remaining_df, filtered)


@pytest.fixture()
def df2(session, data):
    my_new_schema = StructType([
                        StructField('id', IntegerType(), nullable=False),
                        StructField('name', StringType(), nullable=False),
                        StructField('surname', StringType(), nullable=False),
                        ])
    return session.createDataFrame(data=data, schema=my_new_schema)


def test_renamed_columns_are_reachable(df, df2):
    column_remap = {'first_name': 'name', 'last name': 'surname'}
    df = main.rename_columns(df, column_remap)
    assert_df_equality(df, df2)


def test_renaming_missing_cilumns_is_ignored(df):
    column_remap = {'my_first_name': 'my name', 'my last name': 'my surname'}
    df2 = main.rename_columns(df, column_remap)
    assert_df_equality(df2, df)
    