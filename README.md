# Social-On-GitHub

Project Idea:
Find the repositories that the users you follow on GitHub are working on based on the topics

Data Sources:
1. GitHubArchive: This dataset on Google BigQuery gives you event based information on GitHub. The data is segregated based on year, month and day. The dataset is updated on a daily basis.
The data per year is ~ 1TB.
2. GitHub Developer API: The GitHub Developer API gives the information of all the users and repos information.

Technologies Involved:
1. Amazon S3: The data from GitHub Arcive is brought into S3 buckets for batch processing.
2. Spark SQL: The GitHub Archive data consists of inconsistent schema and needs filtering and batch processing. The information from the other sources gives you the relation between users to the users they follow, users to the repositories that they commit on, repositories and their corresponding topics. The data to be sent to the database needs to be remodeled and processed.
3. Kafka: The data from GitHub API is fed through Kafka to Spark.
4. Cassandra/Redis: The data from Spark is kept in three tables(User to Users, User to Repos, Repos to Topics).

