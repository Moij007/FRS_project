from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

spark = SparkSession.builder.appName("FRS_project").getOrCreate()

model_collateral_raw = spark.read.option("header",True).option("inferSchema",True).csv("C:\\Users\\Prime\\OneDrive\\Documents\\FRS_project\\data\\data\\model_collateral.csv")
model_collateral_raw.printSchema()

model_auth_raw = spark.read.option("header",True).option("inferSchema",True).csv("C:\\Users\\Prime\\OneDrive\\Documents\\FRS_project\\data\\data\\model_config.csv")
model_auth_raw.printSchema()

#data validation 1
print(type(model_collateral_raw))

#data validaton 1
print(type(model_auth_raw))

#data validation 2 - collateral_raw
n_cols_collateral = len(model_collateral_raw.columns)
print("Expected number of columns:", n_cols_collateral)

row_count_collateral = model_collateral_raw.count()
print("Number of rows:", row_count_collateral)

#data validation 2 - auth_raw
n_cols_auth = len(model_auth_raw.columns)
print("Expected number of columns:", n_cols_auth)

row_count_auth = model_auth_raw.count()
print("Number of rows:", row_count_auth)

#data validation 4 - list of columns for collateral
columns_collateral = model_collateral_raw.columns

print("Columns for model_collateral:", columns_collateral)

#data valication 4 -list of columns for auth
columns_auth = model_auth_raw.columns

print("Columns for model_auth:", columns_auth)

#data validation 5 - data type for columns of collateral
print("Data types for Collateral:",model_collateral_raw.dtypes)

#data validation 5 - data type for columns of auth
print("Data types for auth:",model_auth_raw.dtypes)


#data validation 6 - check duplicates
from pyspark.sql.functions import count

duplicates_collateral = model_collateral_raw.groupBy(model_collateral_raw.columns) \
    .agg(count("*").alias("count")) \
    .filter("count > 1")

duplicates_collateral.show()

#data validation 6 - check duplicates
duplicates_auth = model_auth_raw.groupBy(model_auth_raw.columns) \
    .agg(count("*").alias("count")) \
    .filter("count > 1")

duplicates_auth.show()

#data validation 7 - check null values

from pyspark.sql.functions import col, sum

null_counts_collateral = model_collateral_raw.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in model_collateral_raw.columns
])

null_counts_collateral.show()

from pyspark.sql.functions import col, sum

null_counts_auth = model_auth_raw.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in model_auth_raw.columns
])

null_counts_auth.show()

#data validation 8 get unique columns
unique_columns_collateral = model_collateral_raw.columns.distinct()