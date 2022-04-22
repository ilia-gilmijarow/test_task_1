#! /bin/env python3.7
from collections.abc import Collection
from pyspark.sql import SparkSession
from pyspark.sql import utils, dataframe, functions
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from sys import argv
import os
import logging
from logging.handlers import RotatingFileHandler


#--- https://www.blog.pythonlibrary.org/2014/02/11/python-how-to-create-rotating-logs/ ---
def create_rotating_log(path, size=2000):
    """
    Creates a rotating log
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    # add a rotating handler
    handler = RotatingFileHandler(path, maxBytes=size, backupCount=5)
    logger.addHandler(handler)
    return logger
    

def read_file(file_name, session):
    '''open input file for reading within a session'''
    log.info(f'reading file {file_name}')
    return session.read.option('header', True).csv(file_name)
   
   
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
    log.info(f'filtering on fcolumn_name} with {str(values)}')
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
        log.info(f'renaming column {k} with {v}')
        df = df.withColumnRenamed(k, v)
    return df


log_file = "KommatiPara.log"
log = create_rotating_log(log_file)
log.info('STARTED NEW RUN')

if __name__ == "__main__":

    #Check if we have all arguments and inform user otherwise
    if len(argv) < 4:
        msg = ['please provide 3 space-separated arguments:',
               'the two file names and a comma-separated list of',
               'countries to filter. If country names containspace enclose in parentheses',
               'example:',
               '         main.py dataset_one.csv dataset_two.csv\n']
        print(*msg, sep='\n')
        log.error('too few parameters provided')
        exit(1)
    file_one, file_two, countries = argv[1:4]
    countries = [x.strip() for x in countries.split(',')]
    # read the files
    the_session = SparkSession.builder\
      .master('local')\
      .appName('main')\
      .getOrCreate()
    df_1 = read_file(file_one, the_session)
    df_2 = read_file(file_two, the_session)
    # drop irrelvant columns
    log.info('dropping unneeded columns')
    df_1 = df_1.drop('first_name', 'last_name')
    df_2 = df_2.drop('cc_n')
    # join
    log.info('performing join')
    df_3 = df_1.join(df_2, on='id')
    # filter
    df_3 = filter_df(df_3, 'country', countries)
    # rename columns
    column_name_mapping = {'id':'client_identifier',
                            'btc_a':'bitcoin_address',
                            'cc_t':'credit_card_type'}
    df_3 = rename_columns(df_3, column_name_mapping)
    script_path = os.path.dirname(os.path.realpath(__file__))
    target_path = os.path.join(script_path,'..','client_data')
    log.info(f'saving to {target_path}')
    df_3.coalesce(1).write.mode('overwrite').csv(target_path)
    