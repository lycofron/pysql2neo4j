'''
Created on 24 Apr 2013

@author: theodojo
'''

__all__ = ['conf']

import logging
import os.path
from collections import OrderedDict
from sqlalchemy.engine import url
from urlparse import urlunparse
from strictconfigparser import StrictConfigParser


class _ConfigMan(object):
    #meta-self.configuration
    _CONFIGFILE = "settings.ini"
    _GLOBALSECTION = 'GLOBAL'
    _SQLDBSECTION = 'SQL_DB'
    _GRAPHDBSECTION = 'GRAPH_DB'
    _OFFLINESECTION = 'OFFLINE_MODE'
    _SQLDB_STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user",
                               "password"]
    _LOG_LEVEL_OPTIONS = {'DEBUG': logging.DEBUG, \
                           'INFO': logging.INFO, \
                           'WARNING': logging.WARNING, \
                           'ERROR': logging.ERROR, \
                           'CRITICAL': logging.CRITICAL, }
    _LABEL_TRANSFORM_OPTIONS = ["capitalize"]
    _RELTYPE_TRANSFORM_OPTIONS = ["allcaps"]

    def __init__(self):
        #Default Values
        _defaultVals = OrderedDict(
                          {_ConfigMan._GLOBALSECTION: {'log_level': 'INFO',
                                            'remove_redundant_fields': True,
                                            'dry_run': True,
                                            'label_transform': 'capitalize',
                                            'csv_row_limit': '10000',
                                            'relation_type_transform':
                                                'allcaps',
                                            'periodic_commit_every': None,
                                            'many_to_many_as_relation': '1'},
                           _ConfigMan._SQLDBSECTION: {'port': None},
                           _ConfigMan._GRAPHDBSECTION: {'path': 'db/data/',
                                             'host': 'localhost',
                                             'port': '7474',
                                             'user': None,
                                             'password': None},
                           _ConfigMan._OFFLINESECTION: {'offline_mode': False,
                                             'cypher_script_name':
                                                    'import.cql'}
                       })
        self.configuration = StrictConfigParser(_defaultVals)
        self.configuration.read(self._CONFIGFILE)

        #Optional
        self.LOG_LEVEL = self._LOG_LEVEL_OPTIONS[
                                self.configuration.getenum(self._GLOBALSECTION,
                                                      "log_level",
                                                      self._LOG_LEVEL_OPTIONS)
                                                ]
        logging.basicConfig(format='%(asctime)s: %(levelname)s:%(message)s',
                            level=self.LOG_LEVEL)
        self.LOG = logging

        #Required
        self.CSV_DIRECTORY = self.configuration.get(self._GLOBALSECTION,
                                               "csv_directory")
        if not os.path.isdir(self.CSV_DIRECTORY):
            raise IOError("CSV directory is invalid")

        #Optional, int
        self.CSV_ROW_LIMIT = self.configuration.getint(self._GLOBALSECTION,
                                                  "csv_row_limit")

        #Optional, int
        self.PERIODIC_COMMIT_EVERY =\
            self.configuration.getint(self._GLOBALSECTION,
                                      "periodic_commit_every")

        #Optional, default Capitalize
        self.TRANSFORM_LABEL = \
            self.configuration.getenum(self._GLOBALSECTION, "label_transform",
                             self._LABEL_TRANSFORM_OPTIONS)

        #Optional, default True
        self.REMOVE_REDUNDANT_FIELDS = \
            self.configuration.getboolean(self._GLOBALSECTION,
                                     "remove_redundant_fields")

        #Optional, default True
        self.MANY_TO_MANY_AS_RELATION = \
            self.configuration.getboolean(self._GLOBALSECTION,
                                     "many_to_many_as_relation")

        #Optional, default True
        self.DRY_RUN = \
            self.configuration.getboolean(self._GLOBALSECTION, "dry_run")

        #Optional, default False
        self.OFFLINE_MODE = \
            self.configuration.getboolean(self._OFFLINESECTION, "offline_mode")

        if self.OFFLINE_MODE:
            _cypher_script_path =\
                self.configuration.get(self._OFFLINESECTION,
                                       "cypher_script_path",
                                       self.CSV_DIRECTORY)
            _cypher_script_name =\
                self.configuration.get(self._OFFLINESECTION,
                                       "cypher_script_name")
            self.CYPHER_SCRIPT_PATH = os.path.join(_cypher_script_path, \
                                               _cypher_script_name)
            self.TARGET_CSV_DIRECTORY =\
                self.configuration.get(self._OFFLINESECTION,
                                       "target_csv_directory",
                                  self.CSV_DIRECTORY)
        else:
            self.TARGET_CSV_DIRECTORY = self.CSV_DIRECTORY

        #Optional, default ALLCAPS
        self.TRANSFORM_REL_TYPES = \
            self.configuration.get(self._GLOBALSECTION,
                                   "relation_type_transform",
                                   self._RELTYPE_TRANSFORM_OPTIONS)
        self.CYPHER_STREAM = _CypherScript(self)

    def getSqlDbUri(self):
        '''Reads SQL DB self.configuration from settings.ini See:
        http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#database-urls
        Return: sqlalchemy.engine.url.URL: sql db connection string'''
        driver = self.configuration.get(self._SQLDBSECTION, "driver")
        host = self.configuration.get(self._SQLDBSECTION, "host")
        port = self.configuration.getint(self._SQLDBSECTION, "port")
        schema = self.configuration.get(self._SQLDBSECTION, "schema")
        user = self.configuration.get(self._SQLDBSECTION, "user")
        password = self.configuration.get(self._SQLDBSECTION, "password")
        otherOptions = [x for x in self.configuration.items(self._SQLDBSECTION)
                        if x[0] not in self._SQLDB_STANDARD_OPTIONS]
        query = {k: v for (k, v) in otherOptions}
        return url.URL(driver, username=user, password=password,
                       host=host, port=port, database=schema, query=query)

    def __getGraphNetLoc(self):
        '''Get network location of a url from settings.ini.
        Default is localhost:7474'''
        host = self.configuration.get(self._GRAPHDBSECTION, "host")
        port = self.configuration.getint(self._GRAPHDBSECTION, "port")
        return "%s:%s" % (host, port)

    def getGraphDBUri(self):
        '''Get Neo4j URI from settings.ini'''
        protocol = "http"
        netLoc = self.__getGraphNetLoc()
        path = self.configuration.get(self._GRAPHDBSECTION, "path")
        urlComponents = (protocol, netLoc, path, "", "", "")
        return urlunparse(urlComponents)

    def getGraphDBCredentials(self):
        '''Get Neo4j credentials from settings.ini, if there are any'''
        netLoc = self.__getGraphNetLoc()
        user = self.configuration.get(self._GRAPHDBSECTION, "user")
        password = self.configuration.get(self._GRAPHDBSECTION, "password")
        if user and password:
            return (netLoc, user, password)
        else:
            return None


class _CypherScript(object):
    '''Just a wrapper around a file stream to support offline mode.'''

    def __init__(self, conf):
        self.conf = conf
        if self.conf.OFFLINE_MODE and (not self.conf.DRY_RUN):
            self._stream = open(conf.CYPHER_SCRIPT_PATH, "w")

    def __del__(self):
        if self.conf.OFFLINE_MODE and (not self.conf.DRY_RUN) and\
        (not self._stream.closed):
            self._stream.flush()
            self._stream.close()

    def write(self, line):
        self._stream.write(unicode.rstrip(line, ";\n") + ";\n")

conf = _ConfigMan()
