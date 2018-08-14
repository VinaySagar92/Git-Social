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
from datetime import datetime

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
df11 = spark.read.json("s3a://vinaysagar-bucket/2011/2011-*.json.gz")
#df11 = spark.read.json("s3a://vinaysagar-bucket/2011/2011-000000000000.json.gz")

# filtering rows with just the three relevant events
df11_watch = df11.filter("type='WatchEvent'")
df11_fork = df11.filter("type='ForkEvent'")
df11_commit = df11.filter("type='CommitCommentEvent'")

# registering  dataframes as tables to be able to select just the three relevant columns
sqlContext.registerDataFrameAsTable(df11_watch, "df11_watch_table")
sqlContext.registerDataFrameAsTable(df11_commit, "df11_commit_table")
sqlContext.registerDataFrameAsTable(df11_fork, "df11_fork_table")

# creating new dataframes with just the relevant columns
df11_watch_altered = sqlContext.sql("SELECT actor, repo, type, created_at FROM df11_watch_table WHERE actor IS NOT NULL and created_at IS NOT NULL and repo IS NOT NULL")
df11_commit_altered = sqlContext.sql("SELECT actor, repo, type, created_at  FROM df11_commit_table WHERE actor is NOT NULL and created_at IS NOT NULL and repo IS NOT NULL")
df11_fork_altered = sqlContext.sql("SELECT actor, repo, type, created_at FROM df11_fork_table WHERE actor is NOT NULL and created_at IS NOT NULL and repo IS NOT NULL")

#df11_watch_alter = sqlContext.sql("SELECT actor, repo, type, created_at FROM df11_watch_table WHERE actor IS NULL and created_at IS NOT NULL")

# registering dataframes as tables to get a union of all
sqlContext.registerDataFrameAsTable(df11_watch_altered, "df11_watch_altered_table")
sqlContext.registerDataFrameAsTable(df11_commit_altered, "df11_commit_altered_table")
sqlContext.registerDataFrameAsTable(df11_fork_altered, "df11_fork_altered_table")

# unifying tables with filtered events and columns
df11_altered_union = sqlContext.sql("SELECT * from df11_watch_altered_table UNION ALL SELECT * from df11_commit_altered_table UNION ALL SELECT * from df11_fork_altered_table")
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

# getting all the user to repo one-to-one mappings
user_repo_map11 = df11_altered_union.rdd.map(lambda x: {"user": x.actor.login, "time": x.created_at, "repo": x.repo.name}).toDF()
user_repo_mapN = user_repo_map11.na.drop(subset=('user', 'time')).rdd.map(lambda c: {"repo": c[0], "time": c[1], "user": c[2]}).toDF()

print(user_repo_mapN.show())

### Repo to Topic to be used for user to topic mapping
#Topics that are available are given as a list
top = ['3D', 'Ajax', 'Algorithm', 'Amp', 'Android', 'Angular', 'Ansible', 'API', 'Adruino', 'ASP.NET', 'Atom', 'Awesome Lists', 'Amazon Web Services', 'Azure', 'Babel', 'Blockchain', 'Bootstrap', 'Bot', 'C', 'Chrome', 'Chrome extension', 'Command line interface', 'Clojure', 'Code quality', 'Code review', 'Compiler', 'Coninuous integration', 'C++', 'Cryptocurrency', 'Crystal', 'C#', 'CSS', 'Data structures', 'Data visualization', 'Database', 'Deep leaning', 'Dependency management', 'Deployment', 'Django', 'Docker', 'Documentation', '.NET', 'Electron', 'Elixir', 'Emacs', 'Ember', 'Emoji', 'Emulator', 'ES6', 'ESLint', 'Ethereum', 'Express', 'Firebase', 'Firefox', 'Flask', 'Font', 'Framework', 'Front end', 'Game engine', 'Git', 'GitHub API', 'Go', 'Google', 'Gradle', 'GraphQL', 'Gulp', 'Haskell', 'Homebrew', 'Homebridge', 'HTML', 'HTTP', 'Icon font', 'iOS', 'IPFS', 'Java', 'Javascript', 'Jekyll', 'jQuery', 'JSON', 'The Julia Language', 'Jupyter Notebook', 'Koa', 'Kotlin', 'Kubernetes', 'Laravel', 'LaTex', 'Library', 'Linux', 'Localization', 'Lua', 'Machine learning', 'macOS', 'Markdown', 'Mastodon', 'Material design', 'MATLAB', 'Maven', 'Minecraft', 'Mobile', 'Monero', 'MongoDB', 'Mongoose', 'Monitoring', 'MvvmCross', 'MySQL', 'NativeScript', 'Nim', 'Natural language processing', 'Node.js', 'NoSQL', 'npm', 'Objective-C', 'OpenGL', 'Operating System', 'P2P', 'Package manager', 'Language parsing', 'Perl', 'Perl 6', 'Phaser', 'PHP', 'Pixel Art', 'PostgreSQL', 'Project management', 'Publishing', 'PWA', 'Python', 'Qt', 'R', 'Rails', 'Raspberry Pi', 'Ratchet', 'React', 'React Native', 'ReactiveUI', 'Redux', 'REST API', 'Ruby', 'Rust', 'Sass', 'Scala', 'scikit-learn', 'Software-defined networking', 'Security', 'Server', 'Serverless', 'Shell', 'Sketch', 'SpaceVim', 'Spring Boot', 'SQL', 'Storybook', 'Support', 'Swift', 'Symfony', 'Telegram', 'Tensorflow', 'Terminal', 'Terraform', 'Twitter', 'Typescript', 'Ubuntu', 'Unity', 'Unreal Engine', 'Vagrant', 'Vim', 'Virtual Reality', 'Vue.js', 'Wagtail', 'Web Components', 'Web App', 'Webpack', 'Windows', 'Wodplate', 'Wordpress', 'Xamarin', 'XML']
# Creating a RDD by mapping repos with 5 topics from the list of topics
df11_altered = sqlContext.sql("SELECT repo FROM df11_altered_union_table WHERE repo IS NOT NULL")
repo_topic_map = df11_altered.rdd.map(lambda x: (x.repo.name, random.sample(top, 5)))
# Creating the dataframe with topics as a list
repo_topic = repo_topic_map.map(lambda x: {"repo":x[0], "topic":([user for sublist in x[1] for user in sublist])}).toDF()



# Performing an inner join for the user to topic relation on the two dataframes created
df_join = user_repo_mapN.join(repo_topic.select(repo_topic.repo, explode(repo_topic.topic).alias("topics")), user_repo_mapN.repo == repo_topic.repo).toDF("repo", "time", "user", "repo", "topic").select("user", "time", "topic")


### User to User Mapping
# creating a list of users to make a mapping
df11_user = sqlContext.sql("SELECT actor FROM df11_altered_union_table where actor is NOT NULL")
user_map = df11_user.rdd.map(lambda x: {"user": x.actor.login}).toDF().dropna(subset='user')

user_list = [i.user for i in user_map.collect()]
# mapping users to follow 5 other users
user_user_map = df11_user.rdd.map(lambda x: (x.actor.login, random.sample(user_list, 5))).groupByKey()
# collecting the pipelined RDD as a list to be written to casandra table
user_user = user_user_map.map(lambda x: {"username":x[0], "userfollow":([user for sublist in x[1] for user in sublist])})
user_user_db = user_user.toDF().dropna().rdd.map(lambda c: ((c[1], c[0])))
# writing to cassandra table useruser
user_user_db.saveToCassandra("events", "useruser")


### User to Repo Mapping
# grouping all records for a given username to get all repositories that the user is following and has contributed to
user_repo_map = df11_altered_union.rdd.map(splitRepo).combineByKey(comb, merg, mergComb).map(lambda c: ((c[0][0], c[0][1], c[1])))
user_rep = user_repo_map.toDF().na.drop(subset=('_1', '_2')).rdd.map(lambda c: ((c[0], c[1], c[2])))
# writing to cassandra table userrepo
user_rep.saveToCassandra("events", "userrepo")


### User to Topic Mapping
# grouping all records for a given username to get all topics that the user is following and has contributed to
user_topic_map = df_join.rdd.map(splitTopic).combineByKey(comb, merg, mergComb).map(lambda c: ((c[0][0], c[0][1], c[1])))
user_topic_db = user_topic_map.toDF().na.drop(subset=('_1', '_2')).rdd.map(lambda c: ((c[0], c[1], c[2])))
# writing to cassandra table usertopic
user_topic_db.saveToCassandra("events", "usertopic")


### Topic to User Mapping
# grouping all records for a given topic to get all users who are following and has contributed for
topic_user_map = df_join.rdd.map(splitUser).combineByKey(comb, merg, mergComb).map(lambda c: ((c[0][0], c[0][1], c[1])))
topic_user_db = topic_user_map.toDF().na.drop(subset=('_1', '_2')).rdd.map(lambda c: ((c[0], c[1], c[2])))
#print(topic_user_db.toDF().show())
# writing to cassandra table topicuser
topic_user_db.saveToCassandra("events", "topicuser")

#for val in user_user_db:
#  try:
#    print val
#  except Exception as e:
#    print e, val
