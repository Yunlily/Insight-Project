import sys
import pyspark
from bisect import bisect_right
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
    
    sqlContext = SQLContext(sc)

    card_table = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","card_info").option("user","root").option("password","Dapiyanzi123").load()
    
    card_table.createOrReplaceTempView("card_table")
    
#     credit_table.show()
    credit_table = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","credit_info").option("user","root").option("password","Dapiyanzi123").load()
    
    credit_table.createOrReplaceTempView("credit_table")
    
#     sqlContext.sql("""
#         select
#              *
#         from
#             credit_table
#     """).show() 

    credit_start_bd = sc.broadcast(credit_table.select("Credit_start").orderBy("Credit_start").rdd.flatMap(lambda x:x).collect())
    
    def find_le(x):
        i = bisect_right(credit_start_bd.value,x)
        if i:
            return credit_start_bd.value[i-1]
        return None
    
    sqlContext.udf.register("find_le",find_le)
    
    
    sqlContext.sql("""
        select
             a.PAN,b.*
        from
            (select *,find_le(TotalCredit) as Credit_start from card_table) a
        left join credit_table b
        on a.TotalCredit = b.Credit_start
    """).show() 
    sc.stop()
    