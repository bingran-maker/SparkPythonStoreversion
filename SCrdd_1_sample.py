from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = [1,2,3,3]
lines = sc.parallelize(data)
# (1-5)函数名:sample(withReplacement,fraction,[seed])
# (1-5)目的:对RDD采样,以及是否替换
# (1-5)数据:[1,2,3,3]
# (1-5)示例:rdd.sample(False,0.5)
# (1-5)结果:非确定的
results = lines.sample(False,0.5)
out = results.collect()
print(out)


# spark-submit SCrdd_1_filter.py
