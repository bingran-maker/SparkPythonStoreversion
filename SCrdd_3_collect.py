from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (3-1)函数名:collect()
# (3-1)目的:返回RDD中的所有元素
# (3-1)数据:[1,2,3,3]
# (3-1)示例:rdd.collect()
# (3-1)结果:[1,2,3,3]
out = lines.collect()
print(out)


# spark-submit SCrdd_1_filter.py
