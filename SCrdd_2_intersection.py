from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data1 = [1,2,3]
data2 = [3,4,5]
lines1 = sc.parallelize(data1)
lines2 = sc.parallelize(data2)
# (2-2)函数名:intersection()
# (2-2)目的:求两个RDD共同的元素的RDD
# (2-2)数据:[1,2,3]和[3,4,5]
# (2-2)示例:rdd.intersection(other)
# (2-2)结果:[3]
results = lines1.intersection(lines2)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
