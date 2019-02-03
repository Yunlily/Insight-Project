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

    #Add schema
    schema = (StructType().add("Name",StringType(), False)
              .add("Phone", StringType(), False)
              .add("Email", StringType(), False)
              .add("Birthday", StringType(), False)
              .add("Company", StringType(), False)
              .add("Address", StringType(), False)
              .add("City", StringType(), False)
              .add("Postal", StringType(), False)
              .add("Latitude", StringType(), False)
              .add("SSN", StringType(), False)
              .add("PAN", StringType(), False)
              .add("PIN", StringType(), False)
              .add("CVV", StringType(), False)
              .add("Type", StringType(), False)
              .add("Guarantor", StringType(), False)
              .add("G-SSN", StringType(), False)
              .add("Tran_num", StringType(), False)
              .add("Merchant", StringType(), False)
              .add("Time", StringType(), False)
              .add("Status", StringType(), False)
              .add("Consumption Type", StringType(), False)
              .add("CardType", StringType(), False)
              .add("Amount", StringType(), False)
             )
                         
    
    #Create DF using RDD and schema
    rdd = rdd.map(lambda x:x.split(";"))
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(rdd, schema)
    
    #
    df.filter(df['Status'] == "Approved").show()
    
    
    
    
#     Displays the content of the DataFrame to stdout
#     sdf_props = {'user':'root','password':'Dapiyanzi123','driver':'com.mysql.jdbc.Driver'}
#     user_address_info.write.jdbc(
#         url='jdbc:mysql://localhost/card_db',
#         table='user_address_info',
#         mode='append',
#         properties = sdf_props
#     )

    

    sc.stop()
    
    
    