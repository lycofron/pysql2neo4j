'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
from sqlalchemy.engine import url
from urlparse import urlunparse


class Config(object):
    '''
    Read settings.ini and provide simple objects with configuration values
    '''
    CONFIGFILE = "settings.ini"
    GLOBALSECTION = 'GLOBAL'
    SQLDBSECTION = 'SQL_DB'
    GRAPHDBSECTION = 'GRAPH_DB'
    STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user", "password"]

    def __init__(self):
        self.__config = ConfigParser.RawConfigParser()
        self.__config.read(self.CONFIGFILE)
        self.globals = {}
        self.globals["csvdir"] = self.__config.get(self.GLOBALSECTION,
                                                    "csvdir")
        self.globals["csvrowlimit"] = self.__config.getint(self.GLOBALSECTION,
                                                           "csvrowlimit")
        try:
            self.globals['labeltransform'] = \
                self.__config.get(self.GLOBALSECTION, "labeltransform")
        except ConfigParser.NoOptionError:
            self.globals['labeltransform'] = 'capitalize'
        try:
            self.globals['transformRelTypes'] = \
                self.__config.get(self.GLOBALSECTION, "transformRelTypes")
        except ConfigParser.NoOptionError:
            self.globals['transformRelTypes'] = 'allcaps'

    def getSqlDbUri(self):
        driver = self.__config.get(self.SQLDBSECTION, "driver")
        host = self.__config.get(self.SQLDBSECTION, "host")
        try:
            port = self.__config.get(self.SQLDBSECTION, "port")
        except:
            port = None
        schema = self.__config.get(self.SQLDBSECTION, "schema")
        user = self.__config.get(self.SQLDBSECTION, "user")
        password = self.__config.get(self.SQLDBSECTION, "password")
        otherOptions = [x for x in self.__config.items(self.SQLDBSECTION)
                        if x[0] not in self.STANDARD_OPTIONS]
        query = {k: v for (k, v) in otherOptions}
        return url.URL(driver, username=user, password=password,
                       host=host, port=port, database=schema, query=query)

    def __getGraphNetLoc(self):
        try:
            host = self.__config.get(self.GRAPHDBSECTION, "host")
        except:
            host = "localhost"
        try:
            port = self.__config.get(self.GRAPHDBSECTION, "port")
        except:
            port = 7474
        return "%s:%s" % (host, port)

    def getGraphDBUri(self):
        protocol = "http"
        netLoc = self.__getGraphNetLoc()
        try:
            path = self.__config.get(self.GRAPHDBSECTION, "path")
        except:
            path = "db/data/"
        urlComponents = (protocol, netLoc, path, "", "", "")
        return urlunparse(urlComponents)

    def getGraphDBCredentials(self):
        netLoc = self.__getGraphNetLoc()
        try:
            user = self.__config.get(self.GRAPHDBSECTION, "user")
        except:
            user = None
        try:
            password = self.__config.get(self.GRAPHDBSECTION, "password")
        except:
            password = None
        if user and password:
            return (netLoc, user, password)
        else:
            return None
