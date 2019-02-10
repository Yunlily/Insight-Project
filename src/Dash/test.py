from flask import Flask, render_template
from flask_mysqldb import MySQL
import yaml

app = Flask(__name__)

#Configure db
db = yaml.load(open('db.yaml'))
app.config['MYSQL_HOST'] = db['mysql_host']
app.config['MYSQL_USER'] = db['mysql_user']
app.config['MYSQL_PASSWORD'] = db['mysql_password']
app.config['MYSQL_DB'] = db['mysql_db']
mysql = MySQL(app)



@app.route('/')
def index():
    cur = mysql.connection.cursor()
    cur.execute("SELECT * FROM user_info")
    mysql.connection.commit()
    print(cur.description)
    cur.close()
    return "Hello World"

if __name__ == '__main__':
    app.run(debug=True)


