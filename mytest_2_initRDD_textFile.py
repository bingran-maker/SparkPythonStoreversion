
from pyspark import SparkConf,SparkContext
import os
# os.environ是当前操作系统的所有环境变量，字典。
os.environ["SPARK_HOME"]="D:\\spark-2.4.5-bin-hadoop2.7"
os.environ["JAVA_HOME"]="D:\\Java\\jdk1.8.0_201"

# (1)构建上下文
conf=SparkConf().setMaster('local[*]').setAppName('mapUSE')
sc=SparkContext(conf=conf)


# (2)初始化RDD的方法有两个:
# 方法一:直接将内存中的数据进行序列化为RDD,使用parallelize
# 方法二:读取外部文件系统中的数据文件,使用textFile



# (2)读取数据形成RDD，读取本地文件系统中的文件
path= "EnvironmentRequirements.txt"
rdd = sc.textFile(path)

# (2-1)获取当前分区数，local[*],当前系统的逻辑核数,为8
# (2-1)textFile不传值时math.min(default.parallelism, 2)
print(rdd.getNumPartitions())#输出为2

# (2-2)按照分区打印数据
# (2-2)获取数据在分区中的存储情况
print(rdd.glom().collect())

# (2-3)重新分区
print(rdd.repartition(5).glom().collect())

# 运行过程中可以查看http://localhost:4040/



