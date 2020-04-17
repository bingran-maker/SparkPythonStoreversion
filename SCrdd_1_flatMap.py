from pyspark import SparkContext
from pyspark import SparkConf
conf=SparkConf().setMaster('spark://myname:7077').setAppName('mapUSE')
# conf=SparkConf().setMaster('local[2]').setAppName('WordCount')
sc=SparkContext(conf=conf)
# sc.setLogLevel('WARN')

data = ['1 2','3 4']
lines = sc.parallelize(data)
# (1-2)函数名:flatMap()
# (1-2)目的:将函数应用于RDD中的每个元素,将返回的迭代器的所有内容构成新的RDD。通常用来切分单词。
# (1-2)示例:rdd.flatMap(lambda x:x.split(' '))
# (1-2)结果:['1', '2', '3', '4']
results = lines.flatMap(lambda x:x.split(' '))
out = results.collect()
print(out)


# spark-submit SCrdd_1_flatMap.py 
