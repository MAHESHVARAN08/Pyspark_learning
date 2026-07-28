#QUESTION
# A company record its employee's movement In and Out of office in a table. Please note below points about the data:


# 1- First entry for each employee is in
# 2- Every in is succeeded by an out
# 3- Employee can work across days
# Write a SQL to find the number of employees inside the Office at 2019-04-01 19:05:00.


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

# Below is a runnable sample dataset and a step-by-step explanation of the PySpark logic used above.
# 1) Create a sample DataFrame `employee_record_df` with columns (emp_id, action, created_at).
# 2) For each "in" event we find the corresponding "out" event by using a window and `lead(created_at, 1)`.
#    - We partition by emp_id and order by created_at so each "in" row gets the timestamp of the next row as its out_time.
#    - Given the constraints (first row per employee is 'in' and every 'in' is followed by an 'out'), the lead() gives the correct out timestamp.
# 3) Filter the rows to only keep the "in" actions (we only need start intervals).
# 4) Use the target timestamp ("2019-04-01 19:05:00") and check which intervals (created_at -> out_time) contain that timestamp.
#    - The `between(start, end)` check is inclusive of both ends (equivalent to start <= t AND t <= end).
# 5) Count the resulting rows to get the number of employees inside at the target timestamp.

# Sample data that demonstrates several cases (employee works across midnight, employee in exactly at target time, employee after target time)
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_timestamp, col

    spark = SparkSession.builder.appName("emp_inside_demo").getOrCreate()

    data = [
        # emp_id, action, created_at
        (1, 'in',  '2019-04-01 08:00:00'),  # in early, out later -> inside at 19:05
        (1, 'out', '2019-04-01 20:00:00'),

        (2, 'in',  '2019-04-01 18:30:00'),  # in just before 19:05, out after -> inside at 19:05
        (2, 'out', '2019-04-01 19:10:00'),

        (3, 'in',  '2019-03-31 23:00:00'),  # overnight shift: out next day early -> NOT inside at 19:05 on Apr 1
        (3, 'out', '2019-04-02 03:00:00'),

        (4, 'in',  '2019-04-01 19:05:00'),  # in exactly at the target time -> included (between is inclusive)
        (4, 'out', '2019-04-01 22:00:00'),

        (5, 'in',  '2019-04-01 19:06:00'),  # in after target time -> NOT included
        (5, 'out', '2019-04-01 21:00:00'),
    ]

    # create DataFrame and cast created_at to timestamp
    employee_record_df = spark.createDataFrame(data, ["emp_id", "action", "created_at"]) \
        .withColumn("created_at", to_timestamp(col("created_at")))

    # Reuse the same logic as above
    window_spec = Window.partitionBy(col("emp_id")).orderBy(col("created_at"))
    chk_df = employee_record_df.withColumn("out_time", lead(col("created_at"), 1).over(window_spec))
    in_df = chk_df.filter(col("action") == 'in')

    # target timestamp to check
    target_ts = lit('2019-04-01 19:05:00')
    final_df = in_df.filter(target_ts.between(col("created_at"), col("out_time")))

    print("Records of employees 'in' intervals containing target time:")
    final_df.show(truncate=False)

    print("Number of employees inside at 2019-04-01 19:05:00:")
    final_df.select(count("*").alias("no_of_emp_inside")).show()

    # Expected result explanation for the sample data above:
    # - emp_id 1: interval 2019-04-01 08:00:00 -> 2019-04-01 20:00:00 contains 19:05 -> included
    # - emp_id 2: interval 2019-04-01 18:30:00 -> 2019-04-01 19:10:00 contains 19:05 -> included
    # - emp_id 3: interval 2019-03-31 23:00:00 -> 2019-04-02 03:00:00 does NOT contain 2019-04-01 19:05:00 -> not included
    # - emp_id 4: interval 2019-04-01 19:05:00 -> 2019-04-01 22:00:00 contains 19:05 (edge case at start) -> included
    # - emp_id 5: interval 2019-04-01 19:06:00 -> 2019-04-01 21:00:00 starts after target -> not included
    # Therefore the expected count is 3 (emp_id 1, 2, and 4).

    spark.stop()
