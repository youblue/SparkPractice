#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  1 18:32:51 2018

@author: Momoeworld
"""

import os
from pyspark import SparkContext
import json

FILE_DIR = "/Users/Momoeworld/Documents/SparkQuestion"
os.chdir(FILE_DIR);
ad_file = "ads_0502.txt"
budget_file = "budget.txt"

sc = SparkContext(appName="countAdsClicked")
budget = sc.textFile(budget_file)\
    .map(lambda line:(json.loads(line)['campaignId'],json.loads(line)['budget']))
ad = sc.textFile(ad_file)\
    .map(lambda line:(json.loads(line)['campaignId'],(json.loads(line)['bidPrice'],json.loads(line)['adId'])))\
    .join(budget)\
    .map(lambda k:(k[1][0][1],k[1][1]/k[1][0][0]))\
    .sortBy(lambda k:k[1], ascending=False)\
    .coalesce(1)
ad.saveAsTextFile("2.countAdsClicked")
sc.stop()