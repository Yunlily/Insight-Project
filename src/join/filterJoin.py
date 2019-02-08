import sys
import pyspark
from bisect import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import StorageLevel

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
    trans_table = sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost/card_db").option("driver","com.mysql.jdbc.Driver").option("dbtable","trans_info").option("user","root").option("password","Dapiyanzi123").load()
    trans_table.createOrReplaceTempView("trans_table")
    
    distinctPAN = sqlContext.sql("select distinct PAN from card_table").persist(StorageLevel.DISK_ONLY)
#     distinctPAN.show()
    
    filteredTrans = sqlContext.sql("select * from trans_table").join(distinctPAN,trans_table.PAN == distinctPAN.PAN,"leftsemi").persist(StorageLevel.DISK_ONLY)
    
    filteredTrans.createOrReplaceTempView("filter_trans")
#     sqlContext.sql("select * from filter_trans").show()
    
    trans_card = sqlContext.sql("select card_table.*,filter_trans.* from card_table left join filter_trans on card_table.PAN = filter_trans.PAN")
    trans_card.createOrReplaceTempView("trans_card");
    
    sqlContext.sql("select * from trans_card").show()
    sc.stop()
    