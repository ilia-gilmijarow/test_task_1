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
        df = df.withColumnRenamed(k, v)
    
    return df