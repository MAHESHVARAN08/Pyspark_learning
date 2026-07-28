#QUESTION
# A company record its employee's movement In and Out of office in a table. Please note below points about the data:

 

# 1- First entry for each employee is “in”
# 2- Every “in” is succeeded by an “out”
# 3- Employee can work across days
# Write a SQL to find the number of employees inside the Office at “2019-04-01 19:05:00".

 

# Table: employee_record
# +-------------+------------+
# | COLUMN_NAME | DATA_TYPE  |
# +-------------+------------+
# | emp_id      | int        |
# | action      | varchar(3) |
# | created_at  | datetime   |
# +-------------+------------+

#SOLUTION
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *

#creating out time for each in 
window_spec = Window.partitionBy(col("emp_id")).orderBy(col("created_at"))
chk_df = employee_record_df.withColumn("out_time",lead(col("created_at"),1).over(window_spec))

# fetching only in records
in_df = chk_df.filter(col("action")=='in')

#Counting between the time
final_df = in_df.filter(lit('2019-04-01 19:05:00').between(col("created_at"),col("out_time")))

no_of_emp = final_df.select(count("*").alias("no_of_emp_inside"))
no_of_emp.show()
#EXPLANATION
