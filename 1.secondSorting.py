# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
from pyspark import SparkContext
import json

FILE_DIR = "/Users/Momoeworld/Documents/SparkQuestion"
os.chdir(FILE_DIR);
file = "ads_0502.txt"

def getAvgBidPrice(keywordList,price):
    for keyword in keywordList:
        yield (keyword,price)

sc = SparkContext(appName="getAvgBidPrice")
data = sc.textFile(file)\
    .flatMap(lambda line:getAvgBidPrice(json.loads(line)["keyWords"],json.loads(line)["bidPrice"]))\
    .groupByKey()\
    .map(lambda pair:(pair[0],sum(pair[1])/len(pair[1])))\
    .sortBy(lambda k:k[1], ascending=False)\
    .coalesce(1)\

data.saveAsTextFile("1.secondSorting")
sc.stop()
