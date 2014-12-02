'''
Created on 24 Apr 2013

@author: theodojo
'''

import ConfigParser
import logging
from sqlalchemy.engine import url
from urlparse import urlunparse

logging.basicConfig(format='%(asctime)s: %(levelname)s:%(message)s',
                    level=logging.INFO)
LOG = logging

__CONFIGFILE = "settings.ini"
__GLOBALSECTION = 'GLOBAL'
__SQLDBSECTION = 'SQL_DB'
__GRAPHDBSECTION = 'GRAPH_DB'
__STANDARD_OPTIONS = ["driver", "host", "port", "schema", "user", "password"]

__config = ConfigParser.RawConfigParser()
__config.read(__CONFIGFILE)
confDict = {}
confDict["csvdir"] = __config.get(__GLOBALSECTION,
                                            "csvdir")
confDict["csvrowlimit"] = __config.getint(__GLOBALSECTION,
                                                   "csvrowlimit")
confDict["periodiccommitevery"] = __config.getint(__GLOBALSECTION,
                                                   "periodiccommitevery")
try:
    confDict['labeltransform'] = \
        __config.get(__GLOBALSECTION, "labeltransform")
except ConfigParser.NoOptionError:
    confDict['labeltransform'] = 'capitalize'
try:
    confDict['transformRelTypes'] = \
        __config.get(__GLOBALSECTION, "transformRelTypes")
except ConfigParser.NoOptionError:
    confDict['transformRelTypes'] = 'allcaps'


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
