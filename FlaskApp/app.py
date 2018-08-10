from flask import Flask, render_template, request
from cassandra.cluster import Cluster
import os
app = Flask(__name__)
from cassandra import ReadTimeout

cluster = Cluster(['52.43.163.255', '52.11.91.29', '54.70.77.167', '54.69.253.190'])
session = cluster.connect('events')

@app.route("/")
def main():
    return render_template('index.html')

@app.route("/test", methods=["GET", "POST"])
def userrepo():
	user = request.form.get("user")
	print(user)
	if user is not None:	
		query = "SELECT repo from userrepo2011 WHERE username = '"+user+"'"
		future = session.execute_async(query)
		#print(future)
		#print("END")
		try:
			repos = future.result()
		except ReadTimeout:
   			print("Query timed out: ")
   		repo = []
   		#for i in repos:
			#print(i)
			#print("HERE")

		print(repos[0])
   		
   		return render_template("test.html", user = user, response = repos[0])
	return render_template("test.html")

if __name__ == "__main__":
    app.run(
    		host=os.getenv('LISTEN', '0.0.0.0'),
    		port=int(os.getenv('PORT', '8080'))
    	)
