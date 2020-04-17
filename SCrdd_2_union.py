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
# (2-1)函数名:union()
# (2-1)目的:生成一个包含两个RDD中所有元素的RDD
# (2-1)数据:[1,2,3]和[3,4,5]
# (2-1)示例:rdd.union(other)
# (2-1)结果:[1,2,3,4,5]
results = lines1.union(lines2)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
