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
    conf.set('spark.cores.max',60)
    conf.set('spark.executor.memory','5g')
    conf.set('spark.rpc.askTimeout',240)
    conf.set('spark.driver.memory','5g')
    conf.set('spark.dynamicAllocation.enabled',True)
    conf.set('spark.shuffle.service.enabled',True)
    conf.set('spark.task.maxFailures',1)
    conf.set('spark.network.timeout','600s')
    sc = SparkContext(conf=conf)
    
    sqlContext = SQLContext(sc)

    card_table = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","card_info").option("user","root").option("password",**********).load()
    
    card_table.createOrReplaceTempView("card_table")
    
#     score_table.show()
    score_table = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","score_info").option("user","root").option("password",**********).load()
    
    score_table.createOrReplaceTempView("score_table")
    
    score_start_bd = sc.broadcast(score_table.select("Score_start").orderBy("Score_start").rdd.flatMap(lambda x:x).collect())
    

    print(score_start_bd.value)
    #Binary Search to find the start of Credit
    def find_le(x):
        i = bisect_right(score_start_bd.value,x)
        if i:
            return score_start_bd.value[i-1]
        return None

    
    sqlContext.udf.register("find_le",find_le)
    

    print("----------------------first-------------------------")
    sqlContext.sql("""
        select
             a.PAN,a.TotalScore,b.Score_start,b.Percentage,b.Condition
        from
            (select *,find_le(TotalScore) as Score_start from card_table) a
        left join score_table b
        on a.Score_start = b.Score_start
    """).show(10) 

    #Have the same result compared to original method
#     print("-----------------------second-------------------------")
#     sqlContext.sql("""
#         select
#             a.*,b.*
#         from
#             card_table A
#             JOIN score_table B
#             ON A.TotalScore >= B.Score_start
#             AND A.TotalScore <= B.Score_end
#     """).show()
    sc.stop()
    
