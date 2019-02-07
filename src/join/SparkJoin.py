import sys
import pyspark
from bisect import *
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
    
    credit_start_bd = sc.broadcast(credit_table.select("Credit_start").orderBy("Credit_start").rdd.flatMap(lambda x:x).collect())
    

#     print(credit_start_bd.value)
    #Binary Search to find the start of Credit
    def find_le(x):
        i = bisect_right(credit_start_bd.value,x)
        if i:
            return credit_start_bd.value[i-1]
        return None

    
    sqlContext.udf.register("find_le",find_le)
    

    print("----------------------first-------------------------")
    sqlContext.sql("""
        select
             a.PAN,a.TotalCredit,b.Credit_start,b.Percentage,b.Condition
        from
            (select *,find_le(TotalCredit) as Credit_start from card_table) a
        left join credit_table b
        on a.Credit_start = b.Credit_start
    """).show(100) 

    #Have the same result compared to original method
#     print("-----------------------second-------------------------")
#     sqlContext.sql("""
#         select
#             a.*,b.*
#         from
#             card_table A
#             JOIN credit_table B
#             ON A.TotalCredit >= B.Credit_start
#             AND A.TotalCredit <= B.Credit_end
#     """).show()
    sc.stop()
    