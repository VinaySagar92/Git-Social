# importing SparkContext and SQLContext from pyspark for batch processing
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import explode
import random
from cassandra.cluster import Cluster
import os
import pyspark_cassandra
import pyspark
from datetime import datetime

# Creating a Cluster object to connect to Cassandra cluster and keyspace
cluster = Cluster(['54.218.131.115', '54.245.65.143', '54.203.126.6', '52.26.161.169'])
#session = cluster.connect('events')

# Creating SparkSession, Spark Context and SQL Context Objects
spark = SparkSession.builder \
            .appName("S3 READ TEST") \
            .config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "6gb") \
	    .config("spark.sql.join.preferSortMergeJoin", "false") \
	    .getOrCreate()

sc=spark.sparkContext
sqlContext = SQLContext(sc)

# Configuring hadoop and spark context with aws key id and secret access secret key to run Spark job and read from S3
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# reading events data from S3
#df11 = spark.read.json("s3a://vinaysagar-bucket/2017/2017-*.json.gz")
#df11 = spark.read.json("s3a://vinaysagar-bucket/2018/2018-*.json.gz")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df11, "df11_event_table")

# creating new dataframes with just the relevant columns
df11_altered_union = sqlContext.sql("SELECT actor, repo, created_at FROM df11_event_table WHERE type = 'ForkEvent' or type = 'CommitCommentEvent' and actor is NOT NULL and created_at IS NOT NULL and repo IS NOT NULL") \
			.na.drop(subset=('created_at')).persist(pyspark.StorageLevel.MEMORY_ONLY)


sqlContext.registerDataFrameAsTable(df11_altered_union, "df11_altered_union_table")


def splitRepo(a):
	b = datetime.strptime(a.created_at.split(" ")[0], '%Y-%m-%d')
	return ((a.actor.login, b), a.repo.name)

def splitTopic(a):
	b = datetime.strptime(a.time.split(" ")[0], '%Y-%m-%d')
	return ((a.user, b), a.topic)

def splitUser(a):
	b = datetime.strptime(a.time.split(" ")[0], '%Y-%m-%d')
	return ((a.topic, b), a.user)

def comb(a):
	return [a]

def merg(a, b):
	a.append(b)
	return a

def mergComb(a, b):
	a.extend(b)
	return a

def ran(a):
        b = random.choice(top)
        return (a.repo.name, b)

def comb_topic(a):
        return a

def merg_topic(a, b):
	return a


### User to User Mapping
# creating a list of users to make a mapping
df11_user = sqlContext.sql("SELECT actor FROM df11_altered_union_table where actor is NOT NULL").persist(pyspark.StorageLevel.MEMORY_ONLY)
user_map = df11_user.rdd.map(lambda x: {"user": x.actor.login}).toDF().dropna(subset='user').persist(pyspark.StorageLevel.MEMORY_ONLY)

user_list = [i.user for i in user_map.collect()]

def ran_user(a):
        b = random.sample(user_list, 5)
        return (a.actor.login, b)

def merg_user(a, b):
	for i in b:
		a.append(i)
	return a;

# mapping users to follow 5 other users
user_user = df11_user.rdd.map(ran_user).combineByKey(comb_topic, merg_user, mergComb).map(lambda c: {"user": c[0], "userfollow": c[1]}).toDF()
# collecting the pipelined RDD as a list to be written to casandra table
user_user_db = user_user.dropna(subset=('user')).rdd.map(lambda c: ((c[0], c[1])))
# writing to cassandra table useruser
print(user_user_db.toDF().show())
user_user_db.saveToCassandra("events", "useruser")
