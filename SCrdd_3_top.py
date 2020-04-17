from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (3-5)函数名:top(num)
# (3-5)目的:从RDD中返回最前面的num个元素
# (3-5)数据:[1,2,3,3]
# (3-5)示例:rdd.top(2)
# (3-5)结果:[3,3]
out = lines.top(2)
print(out)


# spark-submit SCrdd_1_filter.py


