# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import NaiveBayesModel
import pickle

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

f = open("model/texts.pick")
texts = pickle.load(f)
f.close()

f = open("model/labels.pick")
labels = pickle.load(f)
f.close()

texts = sc.parallelize(texts)
htf = HashingTF(1000)  # Warning!! default value is 2^20
htf.transform(texts)

words = "武将 戦国大名 三英傑 尾張 愛知 古渡 城主 嫡男 尾張 守護代 織田".split()
test_tf = htf.transform(words)

model = NaiveBayesModel.load(sc, "model")
test = model.predict(test_tf)

print test, labels[int(test)].encode('utf-8')
