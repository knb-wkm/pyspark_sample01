from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

sentence_data = sc.parallelize([
    {"text": "sapporo fukushima tokyo nagoya osaka kobe fukuoka", "label": 0.0},
    {"text": "alabama alaska arizona california colorado connecticut", "label": 1.0},
    {"text": "finland france germany greece hungary ireland", "label": 2.0},
])

labels = sentence_data.map(lambda s: s["label"])
texts = sentence_data.map(lambda s: s["text"].split(' '))
htf = HashingTF(35)  # Warning!! default value is 2^20
tf = htf.transform(texts)

idf = IDF().fit(tf)
tfidf = idf.transform(tf)

training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
model = NaiveBayes.train(training)

test_tf = htf.transform(["sushi", "tenpura"])
test = model.predict(test_tf)

# debug
p_tfidf = tfidf.collect()
p_training = training.collect()
p_test_p = test_p.collect()

debug_message()
print p_tfidf
print p_training
print p_test_p
print test
