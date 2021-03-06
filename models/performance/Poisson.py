#
# probabilities of Poisson-distributed events
#

import math


def Pn(rate, interval, n=1):
    """ probability of exactly N events during an interval
        rate -- average event rate
        interval -- sample period of interest
        n -- number of desired events
    """
    expect = float(rate) * interval
    p = math.exp(-expect)
    if n > 0:
        p *= (expect ** n)
        p /= math.factorial(n)
    return p


def PnPlus(rate, interval, n=1):
    """ probability of N or more events during an interval
        rate -- average event rate
        interval -- sample period of interest
        n -- number of desired events
    """
    p = 1.0
    i = 0
    while i < n:
        p -= Pn(rate, interval, i)
        i += 1
    return p
