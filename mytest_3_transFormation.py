# RDD TransFormation转换算子
# RDD提供的一系列算子
# map、filter、distinct、sample、
from pyspark import SparkConf,SparkContext
import os
# os.environ是当前操作系统的所有环境变量，字典。
os.environ["SPARK_HOME"]="D:\\spark-2.4.5-bin-hadoop2.7"
os.environ["JAVA_HOME"]="D:\\Java\\jdk1.8.0_201"

# (1)构建上下文
conf=SparkConf().setMaster('local[*]').setAppName('mapUSE')
sc=SparkContext(conf=conf)


# (2)读取数据形成RDD
listdata=[1,2,3,4,5]
rdd=sc.parallelize(listdata)
print(rdd.collect())


# 运行过程中可以查看http://localhost:4040/



