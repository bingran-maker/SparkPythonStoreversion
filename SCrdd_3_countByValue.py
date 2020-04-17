from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (3-3)函数名:countByValue()
# (3-3)目的:各元素在RDD中出现的次数
# (3-3)数据:[1,2,3,3]
# (3-3)示例:rdd.countByValue()
# (3-3)结果:defaultdict(<class 'int'>, {1: 1, 2: 1, 3: 2})
out = lines.countByValue()
print(out)


# spark-submit SCrdd_1_filter.py
