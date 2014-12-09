'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
import logging
import os.path
from sqlalchemy.engine import url
from urlparse import urlunparse
from pysql2neo4j.utils import fixPath

#meta-configuration
__CONFIGFILE = "settings.ini"
__GLOBALSECTION = 'GLOBAL'
__SQLDBSECTION = 'SQL_DB'
__GRAPHDBSECTION = 'GRAPH_DB'
__OFFLINESECTION = 'OFFLINE_MODE'
__STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user", "password"]
__LOG_LEVEL_OPTIONS = {'DEBUG': logging.DEBUG, \
                       'INFO': logging.INFO, \
                       'WARNING': logging.WARNING, \
                       'ERROR': logging.ERROR, \
                       'CRITICAL': logging.CRITICAL, }

__config = ConfigParser.RawConfigParser()
__config.read(__CONFIGFILE)

#Optional
try:
    LOG_LEVEL = __LOG_LEVEL_OPTIONS[__config.get(__GLOBALSECTION, "log_level")]
except (ConfigParser.NoOptionError, KeyError):
    LOG_LEVEL = logging.INFO

logging.basicConfig(format='%(asctime)s: %(levelname)s:%(message)s',
                    level=LOG_LEVEL)
LOG = logging

#Required
CSV_DIRECTORY = __config.get(__GLOBALSECTION, "csv_directory")

if not os.path.isdir(CSV_DIRECTORY):
    raise IOError("CSV directory is invalid")

#Required, int
CSV_ROW_LIMIT = __config.getint(__GLOBALSECTION, "csv_row_limit")

try:
    PERIODIC_COMMIT_EVERY = __config.getint(__GLOBALSECTION,
                                                   "periodic_commit_every")
except ConfigParser.NoOptionError:
    PERIODIC_COMMIT_EVERY = None

#Optional, default Capitalize
try:
    TRANSFORM_LABEL = \
        __config.get(__GLOBALSECTION, "label_transform")
except ConfigParser.NoOptionError:
    TRANSFORM_LABEL = 'capitalize'

#Optional, default True
try:
    _remove_redundant_fields = \
        __config.getint(__GLOBALSECTION, "remove_redundant_fields")
except (ConfigParser.NoOptionError, ValueError):
    _remove_redundant_fields = 1

REMOVE_REDUNDANT_FIELDS = _remove_redundant_fields == 1

#Optional, default True
try:
    _many_to_many_as_relation = \
        __config.getint(__GLOBALSECTION, "many_to_many_as_relation")
except (ConfigParser.NoOptionError, ValueError):
    _many_to_many_as_relation = 1

MANY_TO_MANY_AS_RELATION = _many_to_many_as_relation == 1

#Optional, default True
try:
    _dry_run = \
        __config.getint(__GLOBALSECTION, "dry_run")
except (ConfigParser.NoOptionError, ValueError):
    _dry_run = 1

DRY_RUN = _dry_run != 0

try:
    OFFLINE_MODE = \
        __config.getint(__OFFLINESECTION, "offline_mode")
except (ConfigParser.NoOptionError, ValueError):
    OFFLINE_MODE = False

if OFFLINE_MODE:
    try:
        _cypher_script_path =\
            __config.get(__OFFLINESECTION, "cypher_script_path")
    except ConfigParser.NoOptionError:
        _cypher_script_path = CSV_DIRECTORY
    try:
        _cypher_script_name =\
            __config.get(__OFFLINESECTION, "cypher_script_name")
    except ConfigParser.NoOptionError:
        _cypher_script_name = "import.cql"
    CYPHER_SCRIPT_PATH = os.path.join(_cypher_script_path, \
                                       _cypher_script_name)
    try:
        TARGET_CSV_DIRECTORY =\
            __config.get(__OFFLINESECTION, "target_csv_directory")
    except ConfigParser.NoOptionError:
        TARGET_CSV_DIRECTORY = CSV_DIRECTORY
else:
    TARGET_CSV_DIRECTORY = CSV_DIRECTORY

#Optional, default ALLCAPS
try:
    TRANSFORM_REL_TYPES = \
        __config.get(__GLOBALSECTION, "transformRelTypes")
except ConfigParser.NoOptionError:
    TRANSFORM_REL_TYPES = 'allcaps'


def getSqlDbUri():
    '''Reads SQL DB configuration from settings.ini
    See: http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#database-urls
    Return: sqlalchemy.engine.url.URL: sql db connection string'''
    driver = __config.get(__SQLDBSECTION, "driver")
    host = __config.get(__SQLDBSECTION, "host")
    try:
        port = __config.get(__SQLDBSECTION, "port")
    except:
        port = None
    schema = __config.get(__SQLDBSECTION, "schema")
    user = __config.get(__SQLDBSECTION, "user")
    password = __config.get(__SQLDBSECTION, "password")
    otherOptions = [x for x in __config.items(__SQLDBSECTION)
                    if x[0] not in __STANDARD_OPTIONS]
    query = {k: v for (k, v) in otherOptions}
    return url.URL(driver, username=user, password=password,
                   host=host, port=port, database=schema, query=query)


def __getGraphNetLoc():
    '''Get network location of a url from settings.ini.
    Default is localhost:7474'''
    try:
        host = __config.get(__GRAPHDBSECTION, "host")
    except:
        host = "localhost"
    try:
        port = __config.get(__GRAPHDBSECTION, "port")
    except:
        port = 7474
    return "%s:%s" % (host, port)


def getGraphDBUri():
    '''Get Neo4j URI from settings.ini'''
    protocol = "http"
    netLoc = __getGraphNetLoc()
    try:
        path = __config.get(__GRAPHDBSECTION, "path")
    except:
        path = "db/data/"
    urlComponents = (protocol, netLoc, path, "", "", "")
    return urlunparse(urlComponents)


def getGraphDBCredentials():
    '''Get Neo4j credentials from settings.ini, if there are any'''
    netLoc = __getGraphNetLoc()
    try:
        user = __config.get(__GRAPHDBSECTION, "user")
    except:
        user = None
    try:
        password = __config.get(__GRAPHDBSECTION, "password")
    except:
        password = None
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