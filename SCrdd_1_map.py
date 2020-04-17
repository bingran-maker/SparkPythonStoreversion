from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (1-1)函数名:map()
# (1-1)目的:将函数应用于RDD中的每个元素，将返回值构成新的RDD
# (1-1)示例:rdd.map(x=>x+1)
# (1-1)结果:{2,3,4,4}
results = lines.map(lambda x:x+1)
out = results.collect()
print(out)


# spark-submit SCrdd_1_map.py
