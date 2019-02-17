from flask import Flask, render_template,url_for,redirect,request
from flask_bootstrap import Bootstrap
from flask_mysqldb import MySQL
import yaml


app = Flask(__name__)
Bootstrap(app)

# Configure db
db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']

mysql = MySQL(app)

@app.route('/', methods=['GET','POST'])
def index():
	if request.method == 'POST':
#        	form = request.form
#		email = form['email']
#		password = form['password']
#		cur = mysql.connection.cursor()
#		cur.execute("INSERT INTO user(email, password) VALUES(%s, %s)", (email,password))
#		mysql.connection.commit()
	return render_template('index.html')


@app.errorhandler(404)
def page_not_found(e):
	return 'This page was not found'



if __name__ == '__main__':
	app.run(host='0.0.0.0')


