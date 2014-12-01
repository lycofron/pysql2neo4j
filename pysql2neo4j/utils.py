'''
Created on 04 May 2013

@author: theodojo
'''

import datetime
unixEpoch = datetime.datetime.utcfromtimestamp(0)


def listUnique(seq):
    '''Unique items in list, preserving order'''
    # Thanks to http://stackoverflow.com/a/480227/2822594
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


#Thanks http://stackoverflow.com/a/11111177/2822594
def unix_time(dt):
    '''returns a timedelta from unix epoch'''
    delta = dt - unixEpoch
    return delta.total_seconds()


def getUnixTime(dt):
    '''returns milliseconds elapsed from unix epoch'''
    return int(unix_time(dt)) * 1000
