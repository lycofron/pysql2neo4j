'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
import logging
import os.path
from collections import OrderedDict
from sqlalchemy.engine import url
from urlparse import urlunparse
import yaml


class CustomConfigParser(object):
    '''Pseudo-inherited from ConfigParser, mostly to modify defaults behaviour:
    a. defaults dictionary is a dict of dicts in a structure d[section][option]
    b. getXXX functions: if an option can not be determined from config file,
    it will return (1) argument defaultValue, if specified (2) value from
    defaults dictionary, without type checking.'''

    def __init__(self, defaults):
        '''Constructor'''
        self._super = ConfigParser.ConfigParser()
        self._super.__init__()
        self._customDefault = defaults

    def __getattr__(self, name):
        '''Redirect all unimplemented methods to ConfigParser'''
        return getattr(self._super, name)

    def toDict(self, withDefaults=False):
        '''Return a dictionary containing all sections and options.'''
        d = OrderedDict({s: {k: v for k, v in self.items(s)}
                         for s in self.sections()})
        if withDefaults:
            defDict = dict()
            for k, v in self._customDefault.items():
                defDict[k] = v.copy()
            for k, v in d.items():
                defDict[k].update(v)
            return defDict
        else:
            return d

    def fromDict(self, d):
        '''Import a two-level dictionary as sections/option-values'''
        for section, subd in d.items():
            self.add_section(section)
            for option, value in subd:
                self.set(section, option, value)

    def defaults(self):
        return self._customDefault

    def get(self, section, option, defaultValue=None):
        '''Return an option.'''
        try:
            return self._super.get(section, option)
        except:
            return defaultValue or self._customDefault[section][option]

    def getint(self, section, option, defaultValue=None):
        '''Return an option.'''
        try:
            return self._super.getint(section, option)
        except:
            return defaultValue or self._customDefault[section][option]

    def getfloat(self, section, option, defaultValue=None):
        '''Return an option.'''
        try:
            return self._super.getfloat(section, option)
        except:
            return defaultValue or self._customDefault[section][option]

    def getboolean(self, section, option, defaultValue=None):
        '''Return an option.'''
        try:
            return self._super.getboolean(section, option)
        except:
            return defaultValue or self._customDefault[section][option]

    def getenum(self, section, option, enum, defaultValue=None):
        '''Return option only if it is in enum iterable.'''
        try:
            retval = self._super.getint(section, option)
            if retval in enum:
                return retval
            else:
                return self._customDefault[section][option]
        except:
            return defaultValue or self._customDefault[section][option]


#meta-configuration
_CONFIGFILE = "../settings.ini"
_GLOBALSECTION = 'GLOBAL'
_SQLDBSECTION = 'SQL_DB'
_GRAPHDBSECTION = 'GRAPH_DB'
_OFFLINESECTION = 'OFFLINE_MODE'
_STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user", "password"]
_LOG_LEVEL_OPTIONS = {'DEBUG': logging.DEBUG, \
                       'INFO': logging.INFO, \
                       'WARNING': logging.WARNING, \
                       'ERROR': logging.ERROR, \
                       'CRITICAL': logging.CRITICAL, }
_LABEL_TRANSFORM_OPTIONS = ["capitalize"]
_RELTYPE_TRANSFORM_OPTIONS = ["allcaps"]

#Default Values
defaultVals = OrderedDict(
                  {_GLOBALSECTION: {'log_level': 'INFO',
                                    'remove_redundant_fields': True,
                                    'dry_run': True,
                                    'label_transform': 'capitalize',
                                    'csv_row_limit': '10000',
                                    'relation_type_transform': 'allcaps',
                                    'periodic_commit_every': None,
                                    'many_to_many_as_relation': '1'},
                   _SQLDBSECTION: {'port': None},
                   _GRAPHDBSECTION: {'path': 'db/data/',
                                     'host': 'localhost',
                                     'port': '7474',
                                     'user': None,
                                     'password': None},
                   _OFFLINESECTION: {'offline_mode': False,
                                     'cypher_script_name': 'import.cql'}
               })


configuration = CustomConfigParser(defaultVals)
configuration.read(_CONFIGFILE)


#Optional
LOG_LEVEL = _LOG_LEVEL_OPTIONS[configuration.getenum(_GLOBALSECTION,\
                                                     "log_level",
                                                     _LOG_LEVEL_OPTIONS)]
logging.basicConfig(format='%(asctime)s: %(levelname)s:%(message)s',
                    level=LOG_LEVEL)
LOG = logging

#Required
CSV_DIRECTORY = configuration.get(_GLOBALSECTION, "csv_directory")
if not os.path.isdir(CSV_DIRECTORY):
    raise IOError("CSV directory is invalid")

#Optional, int
CSV_ROW_LIMIT = configuration.getint(_GLOBALSECTION, "csv_row_limit")

#Optional, int
PERIODIC_COMMIT_EVERY =\
    configuration.getint(_GLOBALSECTION, "periodic_commit_every")

#Optional, default Capitalize
TRANSFORM_LABEL = \
    configuration.getenum(_GLOBALSECTION, "label_transform",
                     _LABEL_TRANSFORM_OPTIONS)

#Optional, default True
REMOVE_REDUNDANT_FIELDS = \
    configuration.getboolean(_GLOBALSECTION, "remove_redundant_fields")

#Optional, default True
MANY_TO_MANY_AS_RELATION = \
    configuration.getboolean(_GLOBALSECTION, "many_to_many_as_relation")

#Optional, default True
DRY_RUN = \
    configuration.getboolean(_GLOBALSECTION, "dry_run")

#Optional, default False
OFFLINE_MODE = \
    configuration.getboolean(_OFFLINESECTION, "offline_mode")

if OFFLINE_MODE:
    _cypher_script_path =\
        configuration.get(_OFFLINESECTION, "cypher_script_path", CSV_DIRECTORY)
    _cypher_script_name =\
        configuration.get(_OFFLINESECTION, "cypher_script_name")
    CYPHER_SCRIPT_PATH = os.path.join(_cypher_script_path, \
                                       _cypher_script_name)
    TARGET_CSV_DIRECTORY =\
        configuration.get(_OFFLINESECTION, "target_csv_directory",\
                          CSV_DIRECTORY)
else:
    TARGET_CSV_DIRECTORY = CSV_DIRECTORY

#Optional, default ALLCAPS
TRANSFORM_REL_TYPES = \
    configuration.get(_GLOBALSECTION, "relation_type_transform",
                 _RELTYPE_TRANSFORM_OPTIONS)


def getSqlDbUri():
    '''Reads SQL DB configuration from settings.ini
    See: http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#database-urls
    Return: sqlalchemy.engine.url.URL: sql db connection string'''
    driver = configuration.get(_SQLDBSECTION, "driver")
    host = configuration.get(_SQLDBSECTION, "host")
    port = configuration.getint(_SQLDBSECTION, "port")
    schema = configuration.get(_SQLDBSECTION, "schema")
    user = configuration.get(_SQLDBSECTION, "user")
    password = configuration.get(_SQLDBSECTION, "password")
    otherOptions = [x for x in configuration.items(_SQLDBSECTION)
                    if x[0] not in _STANDARD_OPTIONS]
    query = {k: v for (k, v) in otherOptions}
    return url.URL(driver, username=user, password=password,
                   host=host, port=port, database=schema, query=query)


def __getGraphNetLoc():
    '''Get network location of a url from settings.ini.
    Default is localhost:7474'''
    host = configuration.get(_GRAPHDBSECTION, "host")
    port = configuration.getint(_GRAPHDBSECTION, "port", 8787)
    return "%s:%s" % (host, port)


def getGraphDBUri():
    '''Get Neo4j URI from settings.ini'''
    protocol = "http"
    netLoc = __getGraphNetLoc()
    path = configuration.get(_GRAPHDBSECTION, "path")
    urlComponents = (protocol, netLoc, path, "", "", "")
    return urlunparse(urlComponents)


def getGraphDBCredentials():
    '''Get Neo4j credentials from settings.ini, if there are any'''
    netLoc = __getGraphNetLoc()
    user = configuration.get(_GRAPHDBSECTION, "user")
    password = configuration.get(_GRAPHDBSECTION, "password")
    if user and password:
        return (netLoc, user, password)
    else:
        return None


class CypherScript(object):
    '''Just a wrapper around a file stream to support offline mode.'''

    def __init__(self):
        if OFFLINE_MODE and (not DRY_RUN):
            self._stream = open(CYPHER_SCRIPT_PATH, "w")

    def __del__(self):
        if OFFLINE_MODE and (not DRY_RUN) and (not self._stream.closed):
            self._stream.flush()
            self._stream.close()

    def write(self, line):
        self._stream.write(unicode.rstrip(line, ";\n") + ";\n")

CYPHER_STREAM = CypherScript()

if __name__ == '__main__':
    print yaml.dump(configuration.toDict(True), default_flow_style = False)
