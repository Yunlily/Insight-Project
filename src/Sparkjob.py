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
    
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$
    df = spark.read.format("csv").option("header","true").option("delimiter",",").load("s3n://creditcardtransaction/trans.csv")
    df.show()
    
    card_info =  df.select("PAN","Type","Name","PIN","CVV")
    df.printSchema()
    user_info = df.select("SSN","Name","Email","Birthday","Company","Guarantor")
    guarantor_info = df.select("Guarantor","G-SSN","Name","SSN","PAN")
    trans_info = df.select("PAN","Transacton Number","Latitude")
    buy_info = df.select("Name","Date","Transacton Number","Merchant","ConsumType")
    user_address_info = df.select("Name","SSN","City","Postal","Region","Country")
#     Displays the content of the DataFrame to stdout
    sdf_props = {'user':'root','password':'Dapiyanzi123','driver':'com.mysql.jdbc.Driver'}
    user_address_info.write.jdbc(
        url='jdbc:mysql://localhost/card_db',
        table='user_address_info',
        mode='append',
        properties = sdf_props
    )

    spark.stop()
    
    
    