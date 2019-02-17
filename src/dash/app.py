from flask import Flask, render_template,url_for,redirect,request
from flask_bootstrap import Bootstrap
from flask_mysqldb import MySQL
import yaml
import os

app = Flask(__name__)
Bootstrap(app)

# Configure db
db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
mysql = MySQL(app)

app.config['SECRET_KEY'] = os.urandom(24)
@app.route('/', methods=['GET','POST'])
def index():
    return render_template('index.html')

@app.route('/time')
def time_complexity():
    return render_template('time.html')



@app.errorhandler(404)
def page_not_found(e):
    return 'This page was not found'



if __name__ == '__main__':
	app.run(host='0.0.0.0')


