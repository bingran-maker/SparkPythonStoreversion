from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (1-4)函数名:distinct()
# (1-4)目的:去重
# (1-4)数据:[1,2,3,3]
# (1-4)示例:rdd.distinct()
# (1-4)结果:[1,2,3]
results = lines.distinct()
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
