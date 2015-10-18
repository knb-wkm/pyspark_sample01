from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

sentence_data = sc.parallelize([
    {"text": "Spark pyspark mllib", "label": 0.0},
    {"text": "pyspark mllib", "label": 0.0},
    {"text": "spark", "label": 0.0},
    {"text": "Logistic regression", "label": 1.0},
    {"text": "naive bayes", "label": 1.0},
    {"text": "svm tfidf tf-idf", "label": 1.0}
])

labels = sentence_data.map(lambda s: s["label"])
texts = sentence_data.map(lambda s: s["text"].split(' '))
htf = HashingTF()
tf = htf.transform(texts).cache()

idf = IDF().fit(tf)
tfidf = idf.transform(tf)

training, test = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1])).randomSplit([0.7, 0.3], seed=0)
model = NaiveBayes.train(training)
predictionAndLabel = test.map(lambda p : (model.predict(p.features), p.label))
accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()

# debug
p_tfidf = tfidf.collect()
p_training = training.collect()

debug_message()
print p_tfidf
print p_training
