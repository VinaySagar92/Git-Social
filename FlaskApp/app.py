from flask import Flask, render_template, request
from cassandra.cluster import Cluster
import os
app = Flask(__name__)
from cassandra import ReadTimeout
from datetime import datetime
import time
import random

cluster = Cluster(['54.218.131.115', '54.245.65.143', '54.203.126.6', '52.26.161.169'])
session = cluster.connect('events')
fro = ""
to = ""
user_set= set()

@app.route("/")
def main():
    return render_template('index.html')

@app.route("/gitsocial")
def home():
    return render_template('/gitsocial/index.html')

@app.route("/gitsocial", methods=["GET", "POST"])
def usertopic():
        user = request.form.get("user")
	fromd = request.form.get("from-date")
	fro = datetime.strptime(fromd, "%a %b %d %Y").strftime("%Y-%m-%d")
	fromdate = datetime.strptime(fro, "%Y-%m-%d")
	#fromdate = "10" + "/" + fromd.split(" ")[2] + "/" + fromd.split(" ")[3]
	tod = request.form.get("to-date")
	to = datetime.strptime(tod, "%a %b %d %Y").strftime("%Y-%m-%d")
	todate = datetime.strptime(to, "%Y-%m-%d")
        #todate = "8" + "/" + tod.split(" ")[2] + "/" + tod.split(" ")[3]
	print(fromdate)
        if user is not None:
                query = "SELECT userfollow from useruser WHERE username = '"+user+"'"
                future = session.execute_async(query)
                #print(future)
                #print("END")
                try:
                        users = future.result()
                except ReadTimeout:
                        print("Query timed out: ")
                #print(users[0])
                
                for i in users[0][0]:
                        print i
                        user_set.add(i)
                
                topic = set()
                for i in user_set:
                        if i is not None:
                                query = "SELECT topic from usertopic WHERE username = '"+i+"' and time>'"+fro+"' and time<'"+to+"'"
                                future = session.execute_async(query)
                                 #print(future)
                                 #print("END")
                                try:
                                        topics = future.result()
                                except ReadTimeout:
                                        print("Query timed out: ")
                                print("Here***")
                                print(topics)           
                                for n in topics: 
                                        for a in n: 
                                                for e in a:
                                                        topic.add(e)

                return render_template("gitsocial/index.html", user = user, response = topic)
        return render_template("/gitsocial/index.html")
@app.route("/gitsocial/topic", methods=["GET", "POST"])
def topicuser():
        topic = request.form.get("topic")
#        fromd = request.form.get("from-date")
#        fro = datetime.strptime(fromd, "%a %b %d %Y").strftime("%Y-%m-%d")
#        fromdate = datetime.strptime(fro, "%Y-%m-%d")
        #fromdate = "10" + "/" + fromd.split(" ")[2] + "/" + fromd.split(" ")[3]
#        tod = request.form.get("to-date")
#        to = datetime.strptime(tod, "%a %b %d %Y").strftime("%Y-%m-%d")
#        todate = datetime.strptime(to, "%Y-%m-%d")
	print(topic)
        if topic is not None:
                query_user = "SELECT username from topicuser WHERE topic = '"+topic+"' and time>'"+fro+"' and time<'"+to+"'"
                future = session.execute_async(query_user)
		print("Here************************")	
		print(fro)
                print(to)
                try:
                        users = future.result()
                except ReadTimeout:
                        print("Query timed out: ")
                #print(users[0][0][0])
                userlist = set()
                for i in users:
                        for j in i:
                            for e in j:
                                if len(userlist) < 5:
                                	userlist.add(e)

                return render_template("gitsocial/index.html", topic = topic, response2 = userlist)
        return render_template("gitsocial/index.html")
if __name__ == "__main__":
    app.run(
                host=os.getenv('LISTEN', '0.0.0.0'),
                port=int(os.getenv('PORT', '8080'))
        )

