# Insight-Project
This is a project during  [Insight Engineering Fellow Program](https://www.insightdataengineering.com/).

### Description
This project is inspired by [Spark Summit East talk by Vida Ha](https://www.youtube.com/watch?v=fp53QhSfQcI).
The main tool is Spark, which will perform batch process and join query on dataframe.
The engineering challenge of this project is to optimize the time and space complexity of two join queries:
1. Join a person information table with a credit score table to figure out each client's credit score.
2. Join a daily transaction table with a card table to add card information to today's transaction information.

### Dataset
The data is [generated](http://generatedata.com).
Data size:
Person info Table: 3,000,000.
Card info Table: 3,000,000,000.
Daily Transaction Table: 100,000,000.

### Architecture
![arch](Image/architecture2.png)
