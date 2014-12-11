'''
Created on 11 Dec 2014

@author: Developer
'''
from ConfigParser import ConfigParser
from collections import OrderedDict

__all__ = ['StrictConfigParser']


class StrictConfigParser(object):
    '''Pseudo-inherited from ConfigParser, mostly to modify defaults behaviour:
    a. defaults dictionary is a dict of dicts in a structure d[section][option]
    b. getXXX functions: if an option can not be determined from config file,
    it will return (1) argument defaultValue, if specified (2) value from
    defaults dictionary, without type checking.'''

    def __init__(self, defaults):
        '''Constructor'''
        self._super = ConfigParser()
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
