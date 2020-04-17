from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (3-2)函数名:count()
# (3-2)目的:RDD中的元素个数
# (3-2)数据:[1,2,3,3]
# (3-2)示例:rdd.count()
# (3-2)结果:4
out = lines.count()
print(out)


# spark-submit SCrdd_1_filter.py
