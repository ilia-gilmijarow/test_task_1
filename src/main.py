


def read_file(file_name, session):
    '''open input file for reading within a session'''
    return session.read.option('compression', 'none').csv(file_name)