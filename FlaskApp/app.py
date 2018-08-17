from flask import Flask, render_template, request
from cassandra.cluster import Cluster
import os
app = Flask(__name__)
from cassandra import ReadTimeout

cluster = Cluster(['52.43.163.255', '52.11.91.29', '54.70.77.167', '54.69.253.190'])
session = cluster.connect('events')

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
        print(user)
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
                                query = "SELECT topic from usertopic WHERE username = '"+i+"'"
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
        print(topic)
        if topic is not None:
                query_user = "SELECT username from topicuser WHERE topic = '"+topic+"'"
                future = session.execute_async(query_user)
                #print(future)
                #print("END")
                try:
                        users = future.result()
                except ReadTimeout:
                        print("Query timed out: ")
                #print(users[0])
                userlist = set()
                for i in users:
                        for j in i:
                            for e in j:
                                if e in user_set:
                                       userlist.add(e)

                return render_template("gitsocial/index.html", topic = topic, response2 = userlist)
        return render_template("gitsocial/index.html")
if __name__ == "__main__":
    app.run(
                host=os.getenv('LISTEN', '0.0.0.0'),
                port=int(os.getenv('PORT', '8080'))
        )

