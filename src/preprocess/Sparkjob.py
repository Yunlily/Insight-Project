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
    rdd_user = sc.textFile("s3n://creditcardtransaction/user.csv")
    header = rdd_user.first()
    rdduser = rdd_user.filter(lambda line: line != header).map(lambda x:x.split("|"))
    
    rdd_card = sc.textFile("s3n://creditcardtransaction/card.csv")
    header = rdd_card.first()
    rddcard = rdd_card.filter(lambda line: line != header).map(lambda x:x.split("|"))
    
    rdd_trans = sc.textFile("s3n://creditcardtransaction/trans.csv")
    header = rdd_trans.first()
    rddtrans = rdd_trans.filter(lambda line: line != header).map(lambda x:x.split("|"))
    
    rdd_credit = sc.textFile("s3n://creditcardtransaction/credit.csv")
    rddcredit = rdd_credit.map(lambda x: x.split("|"))
    
    #Add schema
    schemaString1 = "Name Phone Birthday CardNum Address City Postal"
    schemaString2 = "PAN Name PIN CVV Limits Guarantor CardType TotalCredit"
    schemaString3 = "Name Tran_num PAN Merchant Amount Time Type Status Credit"
    schemaString4 = "Credit_start Credit_end Percentage Condition"
    fields_user = [StructField(field_name,StringType(),False) for field_name in schemaString1.split()]
    fields_card = [StructField(field_name,StringType(),False) for field_name in schemaString2.split()]
    fields_trans = [StructField(field_name,StringType(),False) for field_name in schemaString3.split()]
    fields_credit = [StructField(field_name,StringType(),False) for field_name in schemaString4.split()]
    
#     Create DF using RDD and schema
    schema1 = StructType(fields_user)
    schema2 = StructType(fields_card)
    schema3 = StructType(fields_trans)
    schema4 = StructType(fields_credit)
    sqlContext = SQLContext(sc)
    user_info = sqlContext.createDataFrame(rdduser, schema1)
    card_info = sqlContext.createDataFrame(rddcard, schema2)
    trans_info = sqlContext.createDataFrame(rddtrans, schema3)
    credit_info = sqlContext.createDataFrame(rddcredit, schema4)
    
#    Displays the content of the DataFrame to stdout    
    user_info.show()
    card_info.show()
    trans_info.show()
    credit_info.show()
    
    sdf_props = {'user':'root','password':'Dapiyanzi123','driver':'com.mysql.jdbc.Driver'}
    user_info.write.jdbc(
        url='jdbc:mysql://localhost/card_db',
        table='user_info',
        mode='append',
        properties = sdf_props
    )
    card_info.write.jdbc(
        url='jdbc:mysql://localhost/card_db',
        table='card_info',
        mode='append',
        properties = sdf_props
    )
    trans_info.write.jdbc(
        url='jdbc:mysql://localhost/card_db',
        table='trans_info',
        mode='append',
        properties = sdf_props
    )
    credit_info.write.jdbc(
        url='jdbc:mysql://localhost/card_db',
        table='credit_info',
        mode='append',
        properties = sdf_props
    )
    
    sc.stop()
    
