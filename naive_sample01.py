from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

def parseLine(line):
    parts = line.split(',')
    label = float(parts[0])
    features = Vectors.dense([float(x) for x in parts[1].split(' ')])
    return LabeledPoint(label, features)

conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

data = sc.textFile('/usr/local/spark-1.4.1-bin-hadoop2.6/data/mllib/sample_naive_bayes_data.txt').map(parseLine)

# Split data aproximately into training (60%) and test (40%)
training, test = data.randomSplit([0.7, 0.3], seed = 0)

# Train a naive Bayes model.
model = NaiveBayes.train(training, 1.0)
# model.save(sc, "data/naive_bayes.model") # when save model.

# Make prediction and test accuracy.
predictionAndLabel = test.map(lambda p : (model.predict(p.features), p.label))
accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()

# debug_section
p_training =  training.collect()
p_test = test.collect()
p_prediction_label = predictionAndLabel.collect()
p_accuracy = accuracy

# this is debug
debug_message()
print p_training
print p_test
print p_prediction_label
print p_accuracy
