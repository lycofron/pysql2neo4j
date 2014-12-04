'''
Created on 27 Nov 2014

@author: Developer
'''

import unicodecsv as csv
from os import path, devnull

from configman import CSV_DIRECTORY, CSV_ROW_LIMIT, DRY_RUN


def fixPath(path):
    return path.replace('\\', '/')


class CsvHandler(object):
    '''
    Class to handle operations with CSV files
    '''

    def __init__(self, name, header):
        '''
        Constructor
        '''
        self._csvdir = CSV_DIRECTORY
        self._csvRowLimit = CSV_ROW_LIMIT
        self._header = header
        self._name = name
        self._volumeNo = 1
        self._filesWritten = []
        self._getWriter()

    def _getWriter(self):
        self._csvFileName = path.join(self._csvdir,
                                   self._name + "%d.csv" %
                                   self._volumeNo)
        if DRY_RUN:
            self._csvFile = open(devnull, "wb")
        else:
            self._csvFile = open(self._csvFileName, "wb")
        self._csvRowCounter = 0
        self._csvWriter = csv.writer(self._csvFile)
        #Header
        self._csvWriter.writerow(self._header)

    def _next(self):
        self.close()
        self._volumeNo += 1
        self._getWriter()

    def close(self):
        self._filesWritten.append(fixPath(self._csvFileName))
        self._csvFile.close()

    def writeRow(self, row):
        self._csvRowCounter += 1
        if self._csvRowCounter > self._csvRowLimit:
            self._csvRowCounter = 1
            self._next()
        self._csvWriter.writerow(row)

    def getFilesWritten(self):
        return self._filesWritten
