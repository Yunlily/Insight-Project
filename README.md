# Insight-Project
This is a project during  [Insight Engineering Fellow Program](https://www.insightdataengineering.com/).

### Description
Suppose you have a credit card and your spending habit will be learned including how much amount you often spend, location, and frequency you spend.

Based on some ML model, your next transaction could be predicted as fraud if it has significant different from your habit, otherwise it will be predicted as a normal transaction.

A fraud transaction should be alerted in the real-time dashborad.

Since prediction will be done on millions of transaction. Distributed frameworks are used which can scale as the transaction increases.

### Plan
Implementation of  Real time Credit card Fraud Detection using Spark Kafka and Cassandra. 

Implementation of Spark ML Pipeline Stages like String Indexer, One Hot Encoder and Vector Assembler for Pre-processing

### Possible Architecture
![image](http://github.com/Yunlily/Insight-Project/raw/master/Image/Architecture.png)
