import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == "__main__":
    # $example on:init_session$
    
    conf = SparkConf().setAppName("CreditCardInfo")
    sc = SparkContext(conf=conf)
    
    sql = SQLContext(sc)
#     df = sql.read.format("jdbc").options(url="jdbc:mysql:localhost/card_db", driver="com.mysql.jdbc.Driver", dbtable="(SELECT * FROM user_info)",user="root", password="Dapiyanzi123").load()
    df = sql.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","user_info").option("user","root").option("password","Dapiyanzi123").load()
    df.show()
    
    sc.stop()
    