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
    #read raw file from S3 bucket to RDD
    rdd = sc.textFile("s3n://creditcardtransaction/trans.csv")
    rdd1 = rdd.map(lambda x:x.split(";"))

    #Add schema
    schemaString = "Name Phone Email Birthday Company Address City Postal Latitude SSN PAN PIN CVV Type Guarantor G-SSN Tran_num Merchant Date Status Tran_type CardType Amount"
    fields = [StructField(field_name,StringType(),False) for field_name in schemaString.split()]

#     Create DF using RDD and schema
    schema = StructType(fields)
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(rdd1, schema)
    df = df.filter(df['Status'] == "Approved")
    
    
    
    
    
    
    
#     Displays the content of the DataFrame to stdout
#     sdf_props = {'user':'root','password':'Dapiyanzi123','driver':'com.mysql.jdbc.Driver'}
#     user_address_info.write.jdbc(
#         url='jdbc:mysql://localhost/card_db',
#         table='user_address_info',
#         mode='append',
#         properties = sdf_props
#     )

    

    sc.stop()
    
    
    