from pyspark.sql.functions import col
from collections.abc import Collection


def read_file(file_name, session):
    '''open input file for reading within a session'''
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
    if isinstance(values, Collection):
        filtered = df.filter(col(column_name).isin(values))
    else:
        filtered = df.filter(col(column_name) == values)
    return filtered
    