
# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Basic Transformation - I")
    .master("local[*]")
    .getOrCreate()
)

# Emp Data & Schema

emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"
# Create emp DataFrame

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
# Show emp dataframe (ACTION)

emp.show()
+-----------+-------------+-------------+---+------+------+----------+
|employee_id|department_id|         name|age|gender|salary| hire_date|
+-----------+-------------+-------------+---+------+------+----------+
|        001|          101|     John Doe| 30|  Male| 50000|2015-01-01|
|        002|          101|   Jane Smith| 25|Female| 45000|2016-02-15|
|        003|          102|    Bob Brown| 35|  Male| 55000|2014-05-01|
|        004|          102|    Alice Lee| 28|Female| 48000|2017-09-30|
|        005|          103|    Jack Chan| 40|  Male| 60000|2013-04-01|
|        006|          103|    Jill Wong| 32|Female| 52000|2018-07-01|
|        007|          101|James Johnson| 42|  Male| 70000|2012-03-15|
|        008|          102|     Kate Kim| 29|Female| 51000|2019-10-01|
|        009|          103|      Tom Tan| 33|  Male| 58000|2016-06-01|
|        010|          104|     Lisa Lee| 27|Female| 47000|2018-08-01|
|        011|          104|   David Park| 38|  Male| 65000|2015-11-01|
|        012|          105|   Susan Chen| 31|Female| 54000|2017-02-15|
|        013|          106|    Brian Kim| 45|  Male| 75000|2011-07-01|
|        014|          107|    Emily Lee| 26|Female| 46000|2019-01-01|
|        015|          106|  Michael Lee| 37|  Male| 63000|2014-09-30|
|        016|          107|  Kelly Zhang| 30|Female| 49000|2018-04-01|
|        017|          105|  George Wang| 34|  Male| 57000|2016-03-15|
|        018|          104|    Nancy Liu| 29|Female| 50000|2017-06-01|
|        019|          103|  Steven Chen| 36|  Male| 62000|2015-08-01|
|        020|          102|    Grace Kim| 32|Female| 53000|2018-11-01|
+-----------+-------------+-------------+---+------+------+----------+

# Schema for emp

emp.schema
StructType([StructField('employee_id', StringType(), True), StructField('department_id', StringType(), True), StructField('name', StringType(), True), StructField('age', StringType(), True), StructField('gender', StringType(), True), StructField('salary', StringType(), True), StructField('hire_date', StringType(), True)])
# Small Example for Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema_string = "name string, age int"

schema_spark =  StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
# Columns and expression
from pyspark.sql.functions import col, expr

emp["salary"]
Column<'salary'>
# SELECT columns
# select employee_id, name, age, salary from emp

emp_filtered = emp.select(col("employee_id"), expr("name"), emp.age, emp.salary)
# SHOW Dataframe (ACTION)

emp_filtered.show()
+-----------+-------------+---+------+
|employee_id|         name|age|salary|
+-----------+-------------+---+------+
|        001|     John Doe| 30| 50000|
|        002|   Jane Smith| 25| 45000|
|        003|    Bob Brown| 35| 55000|
|        004|    Alice Lee| 28| 48000|
|        005|    Jack Chan| 40| 60000|
|        006|    Jill Wong| 32| 52000|
|        007|James Johnson| 42| 70000|
|        008|     Kate Kim| 29| 51000|
|        009|      Tom Tan| 33| 58000|
|        010|     Lisa Lee| 27| 47000|
|        011|   David Park| 38| 65000|
|        012|   Susan Chen| 31| 54000|
|        013|    Brian Kim| 45| 75000|
|        014|    Emily Lee| 26| 46000|
|        015|  Michael Lee| 37| 63000|
|        016|  Kelly Zhang| 30| 49000|
|        017|  George Wang| 34| 57000|
|        018|    Nancy Liu| 29| 50000|
|        019|  Steven Chen| 36| 62000|
|        020|    Grace Kim| 32| 53000|
+-----------+-------------+---+------+

# Using expr for select
# select employee_id as emp_id, name, cast(age as int) as age, salary from emp_filtered

emp_casted = emp_filtered.select(expr("employee_id as emp_id"), emp.name, expr("cast(age as int) as age"), emp.salary)
# SHOW Dataframe (ACTION)

emp_casted.show()
+------+-------------+---+------+
|emp_id|         name|age|salary|
+------+-------------+---+------+
|   001|     John Doe| 30| 50000|
|   002|   Jane Smith| 25| 45000|
|   003|    Bob Brown| 35| 55000|
|   004|    Alice Lee| 28| 48000|
|   005|    Jack Chan| 40| 60000|
|   006|    Jill Wong| 32| 52000|
|   007|James Johnson| 42| 70000|
|   008|     Kate Kim| 29| 51000|
|   009|      Tom Tan| 33| 58000|
|   010|     Lisa Lee| 27| 47000|
|   011|   David Park| 38| 65000|
|   012|   Susan Chen| 31| 54000|
|   013|    Brian Kim| 45| 75000|
|   014|    Emily Lee| 26| 46000|
|   015|  Michael Lee| 37| 63000|
|   016|  Kelly Zhang| 30| 49000|
|   017|  George Wang| 34| 57000|
|   018|    Nancy Liu| 29| 50000|
|   019|  Steven Chen| 36| 62000|
|   020|    Grace Kim| 32| 53000|
+------+-------------+---+------+

emp_casted_1 = emp_filtered.selectExpr("employee_id as emp_id", "name", "cast(age as int) as age", "salary")
emp_casted_1.show()
+------+-------------+---+------+
|emp_id|         name|age|salary|
+------+-------------+---+------+
|   001|     John Doe| 30| 50000|
|   002|   Jane Smith| 25| 45000|
|   003|    Bob Brown| 35| 55000|
|   004|    Alice Lee| 28| 48000|
|   005|    Jack Chan| 40| 60000|
|   006|    Jill Wong| 32| 52000|
|   007|James Johnson| 42| 70000|
|   008|     Kate Kim| 29| 51000|
|   009|      Tom Tan| 33| 58000|
|   010|     Lisa Lee| 27| 47000|
|   011|   David Park| 38| 65000|
|   012|   Susan Chen| 31| 54000|
|   013|    Brian Kim| 45| 75000|
|   014|    Emily Lee| 26| 46000|
|   015|  Michael Lee| 37| 63000|
|   016|  Kelly Zhang| 30| 49000|
|   017|  George Wang| 34| 57000|
|   018|    Nancy Liu| 29| 50000|
|   019|  Steven Chen| 36| 62000|
|   020|    Grace Kim| 32| 53000|
+------+-------------+---+------+

emp_casted.printSchema()
root
 |-- emp_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- salary: string (nullable = true)

# Filter emp based on Age > 30
# select emp_id, name, age, salary from emp_casted where age > 30

emp_final = emp_casted.select("emp_id", "name", "age", "salary").where("age > 30")
# SHOW Dataframe (ACTION)

emp_final.show()
+------+-------------+---+------+
|emp_id|         name|age|salary|
+------+-------------+---+------+
|   003|    Bob Brown| 35| 55000|
|   005|    Jack Chan| 40| 60000|
|   006|    Jill Wong| 32| 52000|
|   007|James Johnson| 42| 70000|
|   009|      Tom Tan| 33| 58000|
|   011|   David Park| 38| 65000|
|   012|   Susan Chen| 31| 54000|
|   013|    Brian Kim| 45| 75000|
|   015|  Michael Lee| 37| 63000|
|   017|  George Wang| 34| 57000|
|   019|  Steven Chen| 36| 62000|
|   020|    Grace Kim| 32| 53000|
+------+-------------+---+------+

# Write the data back as CSV (ACTION)

emp_final.write.format("csv").save("data/output/2/emp.csv")
# Bonus TIP

schema_str = "name string, age int"

from pyspark.sql.types import _parse_datatype_string

schema_spark = _parse_datatype_string(schema_str)

schema_spark
StructType([StructField('name', StringType(), True), StructField('age', IntegerType(), True)])
 
