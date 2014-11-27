'''
Created on 27 Nov 2014

@author: Developer
'''

import csv
import os.path

wrkDir = r'C:\Users\Developer\Documents\GitHub\pysql2neo4j\csvdir'
csvRowLimit = 1000


def fixPath(path):
    return path.replace('\\', '/')


class CsvHandler(object):
    '''
    Class to handle operations with CSV files
    '''

    def __init__(self, table):
        '''
        Constructor
        '''
        self._table = table
        self._volumeNo = 1
        self._filesWritten = []
        self._getWriter()

    def _getWriter(self):
        csvFileName = os.path.join(wrkDir,
                                   self._table.tablename + "%d.csv" %
                                   self._volumeNo)
        self._csvFile = open(csvFileName, "wb")
        self._csvRowCounter = 0
        self._csvWriter = csv.writer(self._csvFile)
        #Header
        self._csvWriter.writerow(self._table.allcols)

    def _next(self):
        self.close()
        self._volumeNo += 1
        self._getWriter()

    def close(self):
        self._filesWritten.append(fixPath(self._csvFile.name))
        self._csvFile.close()

    def writeRow(self, row):
        self._csvRowCounter += 1
        if self._csvRowCounter > csvRowLimit:
            self._csvRowCounter = 1
            self._next()
        self._csvWriter.writerow(row)

    def getFilesWritten(self):
        return self._filesWritten