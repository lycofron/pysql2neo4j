'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
import logging
from sqlalchemy.engine import url
from urlparse import urlunparse

logging.basicConfig(format='%(asctime)s: %(levelname)s:%(message)s',
                    level=logging.DEBUG)
LOG = logging

__CONFIGFILE = "settings.ini"
__GLOBALSECTION = 'GLOBAL'
__SQLDBSECTION = 'SQL_DB'
__GRAPHDBSECTION = 'GRAPH_DB'
__STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user", "password"]

__config = ConfigParser.RawConfigParser()
__config.read(__CONFIGFILE)

CSV_DIRECTORY = __config.get(__GLOBALSECTION,
                                            "csv_directory")
CSV_ROW_LIMIT = __config.getint(__GLOBALSECTION,
                                                   "csv_row_limit")
PERIODIC_COMMIT_EVERY = __config.getint(__GLOBALSECTION,
                                                   "periodic_commit_every")
try:
    TRANSFORM_LABEL = \
        __config.get(__GLOBALSECTION, "label_transform")
except ConfigParser.NoOptionError:
    TRANSFORM_LABEL = 'capitalize'

try:
    remove_redundant_fields = \
        __config.getint(__GLOBALSECTION, "remove_redundant_fields")
except ConfigParser.NoOptionError:
    remove_redundant_fields = 1
except ValueError:
    remove_redundant_fields = 0

REMOVE_REDUNDANT_FIELDS = remove_redundant_fields == 1

try:
    many_to_many_as_relation = \
        __config.getint(__GLOBALSECTION, "many_to_many_as_relation")
except ConfigParser.NoOptionError:
    many_to_many_as_relation = 1
except ValueError:
    many_to_many_as_relation = 0

MANY_TO_MANY_AS_RELATION = many_to_many_as_relation == 1

try:
    dry_run = \
        __config.getint(__GLOBALSECTION, "dry_run")
except ConfigParser.NoOptionError:
    dry_run = 1
except ValueError:
    dry_run = 1

DRY_RUN = dry_run != 0

try:
    TRANSFORM_REL_TYPES = \
        __config.get(__GLOBALSECTION, "transformRelTypes")
except ConfigParser.NoOptionError:
    TRANSFORM_REL_TYPES = 'allcaps'


def getSqlDbUri():
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
    protocol = "http"
    netLoc = __getGraphNetLoc()
    try:
        path = __config.get(__GRAPHDBSECTION, "path")
    except:
        path = "db/data/"
    urlComponents = (protocol, netLoc, path, "", "", "")
    return urlunparse(urlComponents)


def getGraphDBCredentials():
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
