from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.linalg import _convert_to_vector

def debug_message():
    print "#" * 20 + " debug " + "#" * 20

# conf = SparkConf().setAppName("sample").setMaster("local")
conf = SparkConf().setAppName("sample").setMaster("local")
sc = SparkContext(conf=conf)

sentence_data = sc.parallelize([
    {"text": "sapporo fukushima tokyo nagoya osaka kobe fukuoka", "label": 0.0},
    {"text": "alabama alaska arizona california colorado connecticut", "label": 1.0}
])

labels = sentence_data.map(lambda s: s["label"])
"""
>>> labels.collect()
[0.0, 1.0]
"""

texts = sentence_data.map(lambda s: s["text"].split(' '))
"""
>>> texts.collect()
[['sapporo', 'fukushima', 'tokyo', 'nagoya', 'osaka', 'kobe', 'fukuoka'],
 ['alabama', 'alaska', 'arizona', 'california', 'colorado', 'connecticut']]
"""

htf = HashingTF(1000)  # Warning!! default value is 2^20
tf = htf.transform(texts)
"""
>>> type(tf)
'pyspark.rdd.PipelinedRDD'

>>> tf.collect()
[SparseVector(1000, {241: 1.0, 293: 1.0, 461: 1.0, 561: 1.0, 576: 1.0, 655: 1.0, 778: 1.0}),
 SparseVector(1000, {258: 1.0, 261: 1.0, 325: 1.0, 764: 1.0, 859: 1.0, 914: 1.0})]
"""

idf = IDF().fit(tf)
tfidf = idf.transform(tf)
"""
>>> type(tfidf)
<class 'pyspark.rdd.RDD'>

>>> tfidf.collect()
[SparseVector(1000, {241: 0.4055, 293: 0.4055, 461: 0.4055, 561: 0.4055, 576: 0.4055, 655: 0.4055, 778: 0.4055}),
 SparseVector(1000, {258: 0.4055, 261: 0.4055, 325: 0.4055, 764: 0.4055, 859: 0.4055, 914: 0.4055})]
"""

training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
"""
>>> type(training)
<class 'pyspark.rdd.PipelinedRDD'>

>>> training.collect()
[LabeledPoint(0.0, (1000,[241,293,461,561,576,655,778],[0.405465108108,0.405465108108,0.405465108108,0.405465108108,0.405465108108,0.405465108108,0.405465108108])),
 LabeledPoint(1.0, (1000,[258,261,325,764,859,914],[0.405465108108,0.405465108108,0.405465108108,0.405465108108,0.405465108108,0.405465108108]))]
"""

model = NaiveBayes.train(training)
"""
>>> model.pi
array([-0.69314718, -0.69314718])

>>> model.theta
array([[-6.91058951, -6.91058951, -6.91058951, ..., -6.91058951,
        -6.91058951, -6.91058951],
       [-6.91018512, -6.91018512, -6.91018512, ..., -6.91018512,
        -6.91018512, -6.91018512]])

>>> model.theta[0][241]
-6.5702212286920201

>>> model.theta[0][461]
-6.5702212286920201

>>> model.theta.transpose()
array([[-6.91058951, -6.91018512],
       [-6.91058951, -6.91018512],
       [-6.91058951, -6.91018512],
       ...,)

>>> model.theta.transpose()[241]
array([-6.57022123, -6.91018512])

>>> model.theta.transpose()[461]
array([-6.57022123, -6.91018512])

>>> model.theta.transpose()[0]
array([-6.91058951, -6.91018512])
"""

test_tf = htf.transform(["sapporo", "kobe"])
"""
>>> test_tf
SparseVector(1000, {241: 1.0, 461: 1.0})

>>> test_tf.dot(model.theta.transpose())
array([-13.14044246, -13.82037023])

>>> help(test_tf)
 |  dot(self, other)
 |      Dot product with a SparseVector or 1- or 2-dimensional Numpy array.
 |
 |      >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
 |      >>> a.dot(a)
 |      25.0
 |      >>> a.dot(array.array('d', [1., 2., 3., 4.]))
 |      22.0
 |      >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
 |      >>> a.dot(b)
 |      0.0
 |      >>> a.dot(np.array([[1, 1], [2, 2], [3, 3], [4, 4]]))
 |      array([ 22.,  22.])
 |      >>> a.dot([1., 2., 3.])
 |      Traceback (most recent call last):
 |          ...
 |      AssertionError: dimension mismatch
 |      >>> a.dot(np.array([1., 2.]))
 |      Traceback (most recent call last):
 |          ...
 |      AssertionError: dimension mismatch
 |      >>> a.dot(DenseVector([1., 2.]))
 |      Traceback (most recent call last):
 |          ...
 |      AssertionError: dimension mismatch
 |      >>> a.dot(np.zeros((3, 2)))
 |      Traceback (most recent call last):
 |          ...
 |      AssertionError: dimension mismatch
 |
"""

test = model.predict(test_tf)

# debug
p_tfidf = tfidf.collect()
p_training = training.collect()

debug_message()
print p_tfidf
print p_training
print test
