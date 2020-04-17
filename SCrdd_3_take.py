from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (3-4)函数名:take(num)
# (3-4)目的:从RDD中返回num个元素
# (3-4)数据:[1,2,3,3]
# (3-4)示例:rdd.take(2)
# (3-4)结果:[1,2]
out = lines.take(2)
print(out)


# spark-submit SCrdd_1_filter.py
