#!/bin/bash
    spark-submit \
    --master spark://ec2-54-189-201-125.us-west-2.compute.amazonaws.com:7077\
    --executor-memory 6G\
    --driver-memory 6G\
    --packages anguenot/pyspark-cassandra:0.9.0,com.databricks:spark-csv_2.10:1.2.0\
    --conf spark.cassandra.connection.host=54.218.131.115,54.245.65.143,54.203.126.6,52.26.161.169\
    2011_spark_useruser.py
