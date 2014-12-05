'''
Created on 04 May 2013

@author: theodojo
'''

import datetime
unixEpoch = datetime.datetime.utcfromtimestamp(0)


def listUnique(seq):
    '''Unique items in list, preserving order'''
    # Thanks http://stackoverflow.com/a/480227/2822594
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def listSubtract(seqFrom, seq):
    '''Return all elements in seqFrom that are not in seq,
    preserving order'''
    return [x for x in seqFrom if x not in seq]


def listFlattenIter(seq):
    '''Unnest list. Returns: iterator'''
    for x in seq:
        if hasattr(x, '__iter__'):
            for y in listFlattenIter(x):
                yield y
        else:
            yield x


def listFlatten(seq):
    '''Unnest list. Returns: list'''
    return [x for x in listFlattenIter(seq)]


#Thanks http://stackoverflow.com/a/11111177/2822594
def unix_time(dt):
    '''returns a timedelta from unix epoch'''
    delta = dt - unixEpoch
    return delta.total_seconds()


def getUnixTime(dt):
    '''returns milliseconds elapsed from unix epoch'''
    return int(unix_time(dt)) * 1000


def fixPath(path):
    '''Fix windows backslashes on paths'''
    return path.replace('\\', '/')


def getSubclassesDeep(cls):
    '''Given a class, get all its subclasses from all levels.
    Return: iterator'''
    for subcls in cls.__subclasses__():
        for subsubcls in getSubclassesDeep(subcls):
            yield subsubcls
    yield cls
