'''
Created on 27 Nov 2014

@author: Developer
'''

import unicodecsv as csv
from os import path, devnull

from configman import CSV_DIRECTORY, CSV_ROW_LIMIT, DRY_RUN
from utils import fixPath


class CsvHandler(object):
    '''
    Class to handle operations with CSV files. This is a black box. It receives
    rows of data, it outputs a list of written files.
    '''

    def __init__(self, name, header):
        '''Constructor.
        '''
        self._csvdir = CSV_DIRECTORY
        self._csvRowLimit = CSV_ROW_LIMIT
        self._header = header
        self._name = name
        self._volumeNo = 1
        self._filesWritten = []
        self._getWriter()

    def _getWriter(self):
        ''''Open a new file to write.'''
        #Existing files will be overwritten
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
        '''Proceed to next output file.'''
        self.close()
        self._volumeNo += 1
        self._getWriter()

    def close(self):
        '''Close current file stream.'''
        self._csvFile.close()
        self._filesWritten.append(fixPath(self._csvFileName))

    def writeRow(self, row):
        '''Write a single row, respecting row limits'''
        self._csvRowCounter += 1
        if self._csvRowCounter > self._csvRowLimit:
            self._csvRowCounter = 1
            self._next()
        self._csvWriter.writerow(row)

    def getFilesWritten(self):
        '''Return: all files that have been written and closed so far.'''
        return self._filesWritten
