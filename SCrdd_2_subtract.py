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
# (2-3)函数名:subtract()
# (2-3)目的:移除一个RDD中的内容，例如移除训练数据
# (2-3)数据:[1,2,3]和[3,4,5]
# (2-3)示例:rdd.subtract(other)
# (2-3)结果:[1,2]
results = lines1.subtract(lines2)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
