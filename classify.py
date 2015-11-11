# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import NaiveBayesModel
import pickle
import os, sys

"""
引数に解析させたいキーワードをスペース区切りにて入力
$ spark-submit classify.py "三英傑 一人 海道一"
"""
def debug_message():
    print "#" * 20 + " debug " + "#" * 20

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

path = os.path.abspath(os.path.dirname(__file__))
f = open("%s/model/texts.pick" % path)
texts = pickle.load(f)
f.close()

f = open("%s/model/labels.pick" % path)
labels = pickle.load(f)
f.close()

texts = sc.parallelize(texts)
htf = HashingTF(1000)  # Warning!! default value is 2^20
htf.transform(texts)

words = sys.argv[1].split()
test_tf = htf.transform(words)

model = NaiveBayesModel.load(sc, "%s/model" % path)
test = model.predict(test_tf)

print "label: %s" % labels[int(test)].encode('utf-8')

