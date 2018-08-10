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

# Creating a Cluster object to connect to Cassandra cluster and keyspace
cluster = Cluster(['52.43.163.255', '52.11.91.29', '54.70.77.167', '54.69.253.190'])
#session = cluster.connect('events')

# Creating SparkSession, Spark Context and SQL Context Objects
spark = SparkSession.builder \
            .appName("S3 READ TEST") \
            .config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "6gb") \
            .getOrCreate()

sc=spark.sparkContext
sqlContext = SQLContext(sc)

# Configuring hadoop and spark context with aws key id and secret access secret key to run Spark job and read from S3
hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
hadoop_conf.set("fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])

# reading events data for 2011 from S3
df11 = spark.read.json("s3a://vinaysagar-bucket/2017/2017-*.json.gz")
#df11 = spark.read.json("/usr/local/spark/2011.json.gz")

# filtering rows with just the three relevant events
df11_watch = df11.filter("type='WatchEvent'")
df11_fork = df11.filter("type='ForkEvent'")
df11_commit = df11.filter("type='CommitCommentEvent'")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df11_watch, "df11_watch_table")
sqlContext.registerDataFrameAsTable(df11_commit, "df11_commit_table")
sqlContext.registerDataFrameAsTable(df11_fork, "df11_fork_table")

# creating new dataframes with just the relevant columns
df11_watch_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_watch_table")
df11_commit_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_commit_table")
df11_fork_altered = sqlContext.sql("SELECT actor, repo, type FROM df11_fork_table")

# registering dataframes as tables to get a union of all
sqlContext.registerDataFrameAsTable(df11_watch_altered, "df11_watch_altered_table")
sqlContext.registerDataFrameAsTable(df11_commit_altered, "df11_commit_altered_table")
sqlContext.registerDataFrameAsTable(df11_fork_altered, "df11_fork_altered_table")

# unifying tables with filtered events and columns
df11_altered_union = sqlContext.sql("SELECT * from df11_watch_altered_table UNION ALL SELECT * from df11_commit_altered_table UNION ALL SELECT * from df11_fork_altered_table")
sqlContext.registerDataFrameAsTable(df11_altered_union, "df11_altered_union_table")

# getting all the user to repo one-to-one mappings
#user_repo_map11 = df11_altered_union.rdd.map(lambda x: {"user": x.actor.login, "repo": x.repo.name}).toDF()

### Repo to Topic to be used for user to topic mapping
#Topics that are available are given as a list
#top = ['Java', 'Python', 'API', 'Javascript', 'Ruby', 'Ruby on Rails', 'Scala', 'REST', 'JAX-RS', 'HTML', 'CSS', 'PHP']
# Creating a RDD by mapping repos with 5 topics from the list of topics
#df11_altered = sqlContext.sql("SELECT repo FROM df11_altered_union_table")
#repo_topic_map = df11_altered.rdd.map(lambda x: (x.repo.name, random.sample(top, 5)))
# Creating the dataframe with topics as a list
#repo_topic = repo_topic_map.map(lambda x: {"repo":x[0], "topic":([user for sublist in x[1] for user in sublist])}).toDF()

# Performing an inner join for the user to topic relation on the two dataframes created
#df_join = user_repo_map11.join(repo_topic.select(repo_topic.repo, explode(repo_topic.topic).alias("topics")), user_repo_map11.repo == repo_topic.repo).toDF("repo", "user", "repo", "topic").select("user", "topic")

### User to User Mapping
# creating a list of users to make a mapping
#df11_user = sqlContext.sql("SELECT actor FROM df11_altered_union_table")
#user_map = df11_user.rdd.map(lambda x: {"user": x.actor.login}).toDF()
#user_list = [i.user for i in user_map.collect()]
# mapping users to follow 5 other users
#user_user_map = df11_user.rdd.map(lambda x: (x.actor.login, random.sample(user_list, 5))).groupByKey()
# collecting the pipelined RDD as a list to be written to casandra table
#user_user = user_user_map.map(lambda x: {"user":x[0], "userList":([user for sublist in x[1] for user in sublist])}).collect()

### User to Repo Mapping
# grouping all records for a given username to get all repositories that the user is following and has contributed to
user_repo_map = df11_altered_union.rdd.map(lambda x: (x.actor.login, list([x.repo.name]))).groupByKey()
# collecting the pipelined RDD as a list to be written to casandra table
user_repo = user_repo_map.map(lambda x: {"username":x[0], "repo":[user for sublist in x[1] for user in sublist]})

user_repo.saveToCassandra("events", "userrepo2011")

### User to Topic Mapping
# grouping all records for a given username to get all topics that the user is following and has contributed to
#user_topic_map11 = df_join.rdd.map(lambda x: (x.user, list([x.topic]))).groupByKey()
# collecting the pipelined RDD as a list to be written to casandra table
#user_topic11 = user_topic_map11.map(lambda x: {"username":x[0], "topic":[user for sublist in x[1] for user in sublist]}).collect()

### Topic to User Mapping
# grouping all records for a given topic to get all users who are following and has contributed for
#topic_user_map11 = df_join.rdd.map(lambda x: (x.topic, list([x.user]))).groupByKey()
# collecting the pipelined RDD as a list to be written to casandra table
#topic_user11 = topic_user_map11.map(lambda x: {"topic":x[0], "username":[user for sublist in x[1] for user in sublist]}).collect()

### Writing to Cassandra Tables



# writing all values to cassandra table "userrepo2011"
#for val in user_user:
#  try:
#    session.execute(
#        """
#        INSERT INTO userrepo2011(username, repo)
#        VALUES (%s, %s)
#        """,
#        (val['username'], val['repo'])
#        )
#    print val
#  except Exception as e:
#    print e, val
