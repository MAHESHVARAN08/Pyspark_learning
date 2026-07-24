# #QUESTION
"""
Write an SQL query to determine the transaction date with the lowest average order value (AOV) among all dates recorded in the transaction table. Display the transaction date, its corresponding AOV, and the difference between the highest AOV and the lowest AOV.

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

"""
================================================================================
                    PYSPARK LOGIC EXPLANATION WITH SAMPLE DATA
================================================================================

PROBLEM: Find the transaction date with the LOWEST average order value (AOV)
among all dates, and show the difference from the highest AOV.

SAMPLE INPUT DATA (transactions_df):
+----------+-----------+------------------+---------+
| order_id | user_id   | transaction_date | amount  |
+----------+-----------+------------------+---------+
| 1001     | 101       | 2024-01-01       | 50.00   |
| 1002     | 102       | 2024-01-01       | 75.50   |
| 1003     | 103       | 2024-01-01       | 99.99   |
| 1004     | 104       | 2024-01-02       | 120.00  |
| 1005     | 105       | 2024-01-02       | 150.00  |
| 1006     | 106       | 2024-01-03       | 30.00   |
| 1007     | 107       | 2024-01-03       | 40.00   |
+----------+-----------+------------------+---------+

================================================================================
STEP-BY-STEP BREAKDOWN:
================================================================================

STEP 1: Calculate Average Order Value (AOV) per transaction_date
--------------------------------------------------------------------------
Code:
    aov_df = transactions_df.groupBy(col("transaction_date")).agg(avg(col("transaction_amount")).alias("aov"))

Logic: Groups all transactions by date and calculates the average amount for each date

Result (aov_df):
+------------------+----------+
| transaction_date | aov      |
+------------------+----------+
| 2024-01-01       | 75.16    | (50.00 + 75.50 + 99.99) / 3 = 75.16
| 2024-01-02       | 135.00   | (120.00 + 150.00) / 2 = 135.00
| 2024-01-03       | 35.00    | (30.00 + 40.00) / 2 = 35.00
+------------------+----------+

================================================================================

STEP 2: Find the HIGHEST AOV across all dates
--------------------------------------------------------------------------
Code:
    window_spec1 = Window.partitionBy()  # No partition = entire dataset
    highest_aov = aov_df.withColumn("highest_aov", max(col("aov")).over(window_spec1))

Logic: 
    - Window.partitionBy() with no parameters means we consider the entire dataset
    - max(col("aov")).over(window_spec1) finds the maximum AOV value across all dates
    - max AOV = 135.00 (from 2024-01-02)
    - This value is replicated for all rows

Result (highest_aov):
+------------------+----------+-------------+
| transaction_date | aov      | highest_aov |
+------------------+----------+-------------+
| 2024-01-01       | 75.16    | 135.00      |
| 2024-01-02       | 135.00   | 135.00      |
| 2024-01-03       | 35.00    | 135.00      |
+------------------+----------+-------------+

================================================================================

STEP 3: Rank the dates by AOV in ascending order
--------------------------------------------------------------------------
Code:
    window_spec2 = Window.orderBy(col("aov").asc())
    rn_df = highest_aov.withColumn("rn", row_number().over(window_spec2))

Logic:
    - Window.orderBy(col("aov").asc()) sorts all rows by AOV in ascending order
    - row_number().over(window_spec2) assigns sequential numbers starting from 1
    - Rank 1 = LOWEST AOV (which is what we want!)

Result (rn_df):
+------------------+----------+-------------+----+
| transaction_date | aov      | highest_aov | rn |
+------------------+----------+-------------+----+
| 2024-01-03       | 35.00    | 135.00      | 1  | ← LOWEST AOV
| 2024-01-01       | 75.16    | 135.00      | 2  |
| 2024-01-02       | 135.00   | 135.00      | 3  |
+------------------+----------+-------------+----+

================================================================================

STEP 4: Filter for rank 1 and calculate difference from highest AOV
--------------------------------------------------------------------------
Code:
    final_df = rn_df.filter(col("rn")==1) \
                     .withColumn("diff_from_highest_aov", round(col("highest_aov")-col("aov"), 2)) \
                     .select("transaction_date", round(col("aov"), 2), "diff_from_highest_aov")

Logic:
    - filter(col("rn")==1) keeps only the row with the LOWEST AOV
    - withColumn() calculates the difference: highest_aov (135.00) - aov (35.00) = 100.00
    - round(..., 2) rounds to 2 decimal places
    - select() chooses the final columns to display

FINAL RESULT (final_df):
+------------------+-----+---------------------+
| transaction_date | aov | diff_from_highest_aov|
+------------------+-----+---------------------+
| 2024-01-03       | 35.0| 100.00              |
+------------------+-----+---------------------+

This shows that:
✓ 2024-01-03 has the LOWEST average order value: $35.00
✓ The difference from the highest AOV (135.00) is: $100.00

================================================================================
KEY CONCEPTS:
================================================================================

1. groupBy().agg() → Aggregates data by grouping on one or more columns
2. Window.partitionBy() → Defines a window partition (empty = entire dataset)
3. max().over(window) → Window function that finds max without collapsing rows
4. row_number().over(window) → Assigns sequence numbers within a window
5. filter() → Keeps only rows matching the condition
6. withColumn() → Adds or modifies a column
7. select() → Chooses which columns to display

This approach using window functions is efficient because:
- We don't need multiple passes through the data
- All calculations happen in a single DataFrame transformation pipeline
- Window functions are optimized in Spark's Catalyst optimizer

================================================================================
"""
