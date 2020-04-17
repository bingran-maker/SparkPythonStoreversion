from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (1-3)函数名:filter()
# (1-3)目的:返回一个由通过传给filter()的函数的元素组成的RDD。
# (1-3)数据:[1,2,3,4]
# (1-3)示例:rdd.filter(lambda x:x!=1)
# (1-3)结果:[2,3,3]
results = lines.filter(lambda x:x!=1)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py 
