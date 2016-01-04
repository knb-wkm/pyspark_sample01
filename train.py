# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from multiprocessing import Pool ,cpu_count
import MeCab
import pickle
import os

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
            wakati.append(words[0])

    return wakati


if __name__ == '__main__':

    conf = SparkConf().setAppName("sample").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # processes = Number of Cores
    pool = Pool(processes = 4)

    path = os.path.abspath(os.path.dirname(__file__))
    sentence_data = sc.textFile("%s/data/jawiki-latest-pages-articles.tsv" % path)
    sentence_data.persist()

    labels = sentence_data.map(lambda s: s.split("\t")[0])
    labels.persist()

    multiproc_wakati = sentence_data.map(lambda s: s.split("\t")[1]).collect()
    sentence_data.unpersist()
    texts_temp = pool.map(wakati, multiproc_wakati)
    pool.close()
    pool.join()
    texts = sc.parallelize(texts_temp)

    htf = HashingTF(1000)  # Warning!! default value is 2^20
    tf = htf.transform(texts)
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)

    training = labels.zipWithIndex().map(lambda s: float(s[1])).zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
    model = NaiveBayes.train(training)

    # save naive bayes model
    model.save(sc, ("%s/model" % path))

    # save labels
    labels = labels.collect()
    with open(("%s/model/labels.pick" % path), "w") as f:
        pickle.dump(labels, f)
    labels.unpersist()

    # save texts
    texts = texts.collect()
    with open(("%s/model/texts.pick" % path), "w") as f:
        pickle.dump(texts, f)

