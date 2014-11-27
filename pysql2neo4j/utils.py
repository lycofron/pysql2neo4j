'''
Created on 04 May 2013

@author: theodojo
'''


def listUnique(seq):
    '''Unique items in list, preserving order'''
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
