# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
import MeCab
import pickle

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

def wakati(sentence):
    wakati = []
    mecab = MeCab.Tagger('mecabrc')
    for words in mecab.parse(sentence.encode('utf-8')).split("\n"):
        words = words.split("\t")
        if words[0] == 'EOS' or words[0] == '':
            pass
        else:
            mean = words[1].split(",")[0]
            if mean == '名詞' or mean == '形容詞':
                wakati.append(words[0])
    return wakati

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

sentence_data = sc.textFile("data/jawiki-latest-pages-articles.tsv")
labels = sentence_data.map(lambda s: s.split("\t")[0])
texts = sentence_data.map(lambda s: s.split("\t")[1]).map(lambda s: wakati(s))

htf = HashingTF(1000)  # Warning!! default value is 2^20
tf = htf.transform(texts)
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

training = labels.zipWithIndex().map(lambda s: float(s[1])).zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
model = NaiveBayes.train(training)

# save naive bayes model
model.save(sc, "model")

# save labels
labels = labels.collect()
f = open("model/labels.pick", "w")
pickle.dump(labels, f)
f.close()

# save texts
texts = texts.collect()
f = open("model/texts.pick", "w")
pickle.dump(texts, f)
f.close()
