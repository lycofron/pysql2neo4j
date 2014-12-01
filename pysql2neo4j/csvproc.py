'''
Created on 27 Nov 2014

@author: Developer
'''

import unicodecsv as csv
import os.path

from configman import confDict


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
        self._csvdir = confDict["csvdir"]
        self._csvRowLimit = confDict["csvrowlimit"]
        self._header = header
        self._name = name
        self._volumeNo = 1
        self._filesWritten = []
        self._getWriter()

    def _getWriter(self):
        csvFileName = os.path.join(self._csvdir,
                                   self._name + "%d.csv" %
                                   self._volumeNo)
        self._csvFile = open(csvFileName, "wb")
        self._csvRowCounter = 0
        self._csvWriter = csv.writer(self._csvFile)
        #Header
        self._csvWriter.writerow(self._header)

    def _next(self):
        self.close()
        self._volumeNo += 1
        self._getWriter()

    def close(self):
        self._filesWritten.append(fixPath(self._csvFile.name))
        self._csvFile.close()

    def writeRow(self, row):
        self._csvRowCounter += 1
        if self._csvRowCounter > self._csvRowLimit:
            self._csvRowCounter = 1
            self._next()
        self._csvWriter.writerow(row)

    def getFilesWritten(self):
        return self._filesWritten
