'''
Created on 12 dec 2013

@author: theodojo
'''


class pysql2graphException(Exception):
    '''Base class for all custom exceptions'''

    def __init__(self, ex, addMsg):
        self.message = addMsg + "\n" + ex.message


class DbNotFoundException(pysql2graphException):
    '''Could not connect to database.'''
    pass


class DBUnreadableException(pysql2graphException):
    '''Could not read from database.'''
    pass


class DBInsufficientPrivileges(pysql2graphException):
    '''Could not write on database.'''
    pass
