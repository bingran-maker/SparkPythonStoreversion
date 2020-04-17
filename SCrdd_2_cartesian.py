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
# (2-4)函数名:cartesian()
# (2-4)目的:与另一个RDD的笛卡尔积
# (2-4)数据:[1,2,3]和[3,4,5]
# (2-4)示例:rdd.cartesian(other)
# (2-4)结果:[(1, 3), (1, 4), (1, 5), (2, 3), (3, 3), (2, 4), (2, 5), (3, 4), (3, 5)]
results = lines1.cartesian(lines2)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
