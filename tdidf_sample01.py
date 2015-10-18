from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

sentence_data = sc.parallelize([
    {"text": "Hi I heard about Spark",              "label": 0.0},
    {"text": "I wish Python could use case classes","label": 0.0},
    {"text": "Logistic regression models are neat", "label": 1.0},
    {"text": "naive bayes models are sloppy",       "label": 1.0}
])

labels = sentence_data.map(lambda s: s["label"])
texts = sentence_data.map(lambda s: s["text"].split(' '))
htf = HashingTF()
tf = htf.transform(texts).cache()

idf = IDF().fit(tf)
tfidf = idf.transform(tf)

training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
model = NaiveBayes.train(training)

# debug
p_tfidf = tfidf.collect()
p_training = training.collect()
debug_message()
print p_tfidf
print p_training
