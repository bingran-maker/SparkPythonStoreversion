# Could not find valid SPARK_HOME while searching
# ['D:\\CODE\\pythonproject', 'D:\\Anaconda3\\envs\\python36\\lib\\site-packages\\pyspark',
# 'D:\\Anaconda3\\envs\\python36\\lib\\site-packages\\pyspark', 'D:\\Anaconda3\\envs\\python36\\lib']
# 报错：在当前环境下，找不到SPARK_HOME环境
# 解决方式：解压一个D:\spark-2.4.5-bin-without-hadoop不用任何配置。
# (方式1)配置系统环境变量。
# (方式2)在当前的运行环境中添加参数Run->Edit Configurations->Environment Variables
# (方式3)在代码中添加环境变量
from pyspark import SparkConf,SparkContext
import os
# os.environ是当前操作系统的所有环境变量，字典。
os.environ["SPARK_HOME"]="D:\\spark-2.4.5-bin-hadoop2.7"
os.environ["JAVA_HOME"]="D:\\Java\\jdk1.8.0_201"

# (1)构建上下文
conf=SparkConf().setMaster('local[2]').setAppName('mapUSE')
sc=SparkContext(conf=conf)
# (2)读取数据形成RDD
listdata=[1,2,3,4,5]
rdd=sc.parallelize(listdata)
print(rdd.collect())


