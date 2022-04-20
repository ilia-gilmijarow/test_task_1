#! /bin/env python3.7
from collections.abc import Collection
from pyspark.sql import SparkSession
from pyspark.sql import utils, dataframe, functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from sys import argv
from sys import getcwd
import logging
from logging.handlers import RotatingFileHandler


#--- https://www.blog.pythonlibrary.org/2014/02/11/python-how-to-create-rotating-logs/ ---
def create_rotating_log(path):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    # add a rotating handler
    handler = RotatingFileHandler(path, maxBytes=20, backupCount=5)
    logger.addHandler(handler)
    return logger
    

def read_file(file_name, session):
    '''open input file for reading within a session'''
    log.INFO(f'reading file {file_name}')
    return session.read.option('compression', 'none').csv(file_name)
   
   
def filter_df(df, column_name, values):
    '''
    filter data frame preserving values in the given column_name
    :param DataFrame df: the data frame to be filtered
    :param str column_name: the name of the column to apply the filter
    :param collection or other values: if a collection, e.g. list is provided, 
                                     the .isin(...) method will be called, otherwise 
                                     the equality operator will be used as if
                                     this parameter contains a single value, e.g. a string
    :return: filtered data frame
    :rtype DataFrame:
    '''
    log.INFO('filtering on f{column_name} with f{str(values)}')
    if isinstance(values, Collection):
        filtered = df.filter(functions.col(column_name).isin(values))
    else:
        filtered = df.filter(functions.col(column_name) == values)
    return filtered


def rename_columns(df, column_name_mapping):
    '''
    return a data frame with the same data, but with columns renamed according to mapping
    :param DataFrame df: the data frame
    :param dict or another mapping column_name_mapping: mapping where keys are existing column names, 
                                                        and their values are the new column names
                                                        if there are superfluous keys in the mapping they are ignored.
    :return: data frame with new column names
    :rtype DataFrame:
    '''
    
    for k, v in column_name_mapping.items():
        log.INFO('renaming column f{k} with f{v}')
        df = df.withColumnRenamed(k, v)
    return df
    

if __name__ == "__main__":
    log_file = "test.log"
    log = create_rotating_log(log_file)
    #Check if we have all arguments and inform user otherwise
    if len(argv) < 4:
        msg = ['please provide 3 space-separated arguments:',
               'the two file names and a comma-separated list of',
               'countries to filter. If country names containspace enclose in parentheses',
               'example:',
               '         main.py dataset_one.csv dataset_two.csv\n']
        print(*msg, sep='\n')
        log.ERROR('too few parameters provided')
        exit(1)
    file_one, file_two, countries = argv[1:4]
    # read the files
    
    # rename columns
    
    # drop irrelvant columns
    
    # join
    
    
    