# #QUESTION
"""
Write an SQL query to determine the transaction date with the lowest average order value (AOV) among all dates recorded in the transaction table. Display the transaction date, its corresponding AOV, and the difference between the AOV for that date and the highest AOV for any day in the dataset. Round the result to 2 decimal places.

 

Table: transactions 
+--------------------+--------------+
| COLUMN_NAME        | DATA_TYPE    |
+--------------------+--------------+
| order_id           | int          |
| transaction_amount | decimal(5,2) |
| transaction_date   | date         |
| user_id            | int          |
+--------------------+--------------+
"""
#SOLUTION
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# taking average
aov_df = transactions_df.groupBy(col("transaction_date")).agg(avg(col("transaction_amount")).alias("aov"))

# finding highest average
window_spec1 = Window.partitionBy()
highest_aov = aov_df.withColumn("highest_aov",max(col("aov")).over(window_spec1))


# ranking averages
window_spec2 = Window.orderBy(col("aov").asc())
rn_df = highest_aov.withColumn("rn",row_number().over(window_spec2))


#final_df
final_df=rn_df.filter(col("rn")==1).withColumn("diff_from_highest_aov",round(col("highest_aov")-col("aov"),2))\
			.select("transaction_date",round(col("aov"),2),"diff_from_highest_aov")
final_df.show() 

#EXPLANATION

