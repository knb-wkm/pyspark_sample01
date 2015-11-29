# -*- coding: utf-8 -*-
from flask import Flask, render_template, request
import re
import commands

"""
メモ1: /classifyアクションにてsshログインするため
spark masterに対して認証なしでログインする設定を行っておくこと
メモ2: spark/conf/log4j.propertiesにて出力レベルをERRORにしておくこと
"""
app = Flask(__name__)
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/face')
def face():
    return render_template("face.html")

@app.route('/classify', methods=['POST'])
def classify():
    if request.method == 'POST':
        keywords = request.form["keywords"].encode('utf-8')
        spark_submit = "/usr/local/spark/bin/spark-submit"
        classify_path = "repos/python/pyspark_sample_github/classify.py"
        cmd = 'ssh localhost "%s %s %s"' % (spark_submit, classify_path, keywords)
        result = commands.getoutput(cmd)
        p = re.compile('label: ')
        filtered_result = [row for row in result.split("\n") if p.match(row)]
        return "".join(filtered_result)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
