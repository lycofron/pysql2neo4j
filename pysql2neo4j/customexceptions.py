'''
Created on 12 dec 2013

@author: theodojo
'''


class pysql2graphException(Exception):

    def __init__(self, ex, addMsg):
        self.message = addMsg + "\n" + ex.message


class DbNotFoundException(pysql2graphException):
    pass


class DBUnreadableException(pysql2graphException):
    pass


class DBInsufficientPrivileges(pysql2graphException):
    pass


class WorkflowException(pysql2graphException):
    pass
