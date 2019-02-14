spark-submit --master local[12] --driver-memory 20g --conf spark.network.timeout=600s --jars /usr/share/java/mysql-connector-java.jar RangeJoin.py 
