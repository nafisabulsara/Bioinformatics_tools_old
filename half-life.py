#!/usr/bin/python
from __future__ import division
import numpy as np
from scipy import stats
import csv
import sys
from operator import itemgetter

filename = "DecayTimecourse.txt"

line = []
tclist = []
ylist = []
set1 = []
set2 = []
set3 = []
halflife1 = {}
halflife2 = {}
halflife3 = {}
h_averages = {}


# xlist should be a list of x values
# https://docs.python.org/2/tutorial/controlflow.html

# Slope of the line
def slope(x, y):
    if len(x) == 0:
        return 'NA'
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    return slope


def halflife(slope):
    if slope == 'NA':
        return 'NA'
    hl = 0.693 / (slope)
    return hl


def getHalfLifeInSet(xlist, ylist):
    i = 0
    valid_set_xlist = []
    valid_set_ylist = []
    while i < len(ylist):
        if ylist[i] != '':
            valid_set_xlist.append(float(xlist[i]))
            valid_set_ylist.append(float(ylist[i]))
        i = i + 1
    # Get half life
    hlflyf = halflife(slope(valid_set_xlist, valid_set_ylist))
    return hlflyf


def getAverage(i1, i2, i3):
    items = []
    if i1 != 'NA':
        items.append(i1)
    if i2 != 'NA':
        items.append(i2)
    if i3 != 'NA':
        items.append(i3)

    if len(items) > 0:
        return sum(items) / float(len(items))
    else:
        return 'NA'


# Open and read file
with open(filename, 'r') as f:
    for line in f:
        line = line.strip()
        tmp = line.split('\t')
        # Exclude first line
        if line.startswith('Time'):
            continue
        # Store header line in static list 'xlist'
        if line.startswith('YORF'):
            tclist = tmp[1:10]
            continue
        # Store 3 sets of values in 3 lists
        gene = tmp[0]
        set1 = tmp[1:10]
        set2 = tmp[10:19]
        set3 = tmp[19:28]
        # Capture non blank values in each set
        hl_set1 = getHalfLifeInSet(tclist, set1)
        hl_set2 = getHalfLifeInSet(tclist, set2)
        hl_set3 = getHalfLifeInSet(tclist, set3)

        # print gene + '\t' + str(hl_set1) + '\t' + str(hl_set2) + '\t' + str(hl_set3)
        average = getAverage(hl_set1, hl_set2, hl_set3)
        h_averages[gene] = average

# Calculate number of items that would be 10% of collection
subset_averages_len = int(len(h_averages.keys()) / 10)

abc = sorted(h_averages.keys(), key=h_averages.get)

top10avg = 'top10averages.tsv'
bot10avg = 'bot10averages.tsv'
allavg = 'allaverages.tsv'

i = 0
with open(bot10avg, 'w') as bt, open(top10avg, 'w') as tt, open(allavg, 'w') as aa:
    while i < len(abc):
        if isinstance(h_averages[abc[i]], float) and i < subset_averages_len:
            bt.write(abc[i] + '\t' + str(h_averages[abc[i]]) + '\n')
        if isinstance(h_averages[abc[i]], float) and i > (len(abc) - subset_averages_len):
            tt.write(abc[i] + '\t' + str(h_averages[abc[i]]) + '\n')
        aa.write(abc[i] + '\t' + str(h_averages[abc[i]]) + '\n')
        i = i + 1
