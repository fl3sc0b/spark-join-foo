# Databricks notebook source
# MAGIC %md
# MAGIC # spark-join-foo
# MAGIC ## A set of examples for each of the seven types of joins in spark
# MAGIC ### Databricks pyspark notebook version

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe A

# COMMAND ----------

sch_dfA = StructType(
    [
        StructField('key', IntegerType(), False),
        StructField('col1_dfA', StringType(), True),
        StructField('col2_dfA', StringType(), True)
    ]
)

data_dfA = [
    (1, 'abc', 'def'),
    (2, '123', '456'),
    (3, 'xxx', 'yyy'),
    (3, 'dupxxx', 'dupyyy'), # row partially duplicated
    (5, 'a1b1', 'c2d2'),
    (5, 'a1b1', 'c2d2') # row fully duplicated
]

dfA = spark.createDataFrame(schema = sch_dfA, data = data_dfA)
dfA.createOrReplaceTempView('dfA') # sent to spark SQL

# COMMAND ----------

display(dfA)

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA |
# MAGIC | --- | -------- | -------- |
# MAGIC | 1 | abc | def |
# MAGIC | 2 | 123 | 456 |
# MAGIC | 3 | xxx | yyy |
# MAGIC | 3 | dupxxx | dupyyy |
# MAGIC | 5 | a1b1 | c2d2 |
# MAGIC | 5 | a1b1 | c2d2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe B

# COMMAND ----------

sch_dfB = StructType(
    [
        StructField('key', IntegerType(), False),
        StructField('col1_dfB', StringType(), True),
        StructField('col2_dfB', StringType(), True)
    ]
)

data_dfB = [
    (1, 'cbs', 'fed'),
    (2, '321', '654'),
    (2, 'dup321', 'dup654'), # row partially duplicated
    (4, '???', '!!!'),
    (5, '1b1a', '2d2c'),
    (6, 'wtf', 'wow')
]

dfB = spark.createDataFrame(schema = sch_dfB, data = data_dfB)
dfB.createOrReplaceTempView('dfB') # sent to spark SQL

# COMMAND ----------

display(dfB)

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- |
# MAGIC | 1 | cbs | fed |
# MAGIC | 2 | 321 | 654 |
# MAGIC | 2 | dup321 | dup654 |
# MAGIC | 4 | ??? | !!! |
# MAGIC | 5 | 1b1a | 2d2c |
# MAGIC | 6 | wtf | wow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner join
# MAGIC **Description:** ROWS composed by COLUMNS from BOTH DataFrames AND WHERE CONDITION MATCHES

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'inner'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC INNER JOIN dfB B
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- | --- | -------- | -------- |
# MAGIC | 1 | abc | def | 1 | cbs | fed |
# MAGIC | 2 | 123 | 456 | 2 | 321 | 654 |
# MAGIC | 2 | 123 | 456 | 2 | dup321 | dup654 |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left join
# MAGIC **Description:** A DataFrame INNER JOIN B DataFrame UNION ROWS composed by COLUMNS from A DataFrame WITH NULL VALUES for COLUMNS in the B DataFrame WHERE CONDITION DOES NOT MATCH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'left')) # synonyms: 'leftouter', 'left_outer'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC LEFT JOIN dfB B -- synonyms: 'LEFT OUTER JOIN'
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- | --- | -------- | -------- |
# MAGIC | 1 | abc | def | 1 | cbs | fed |
# MAGIC | 2 | 123 | 456 | 2 | 321 | 654 |
# MAGIC | 2 | 123 | 456 | 2 | dup321 | dup654 |
# MAGIC | 3 | xxx | yyy | null | null | null |
# MAGIC | 3 | dupxxx | dupyyy | null | null | null |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Right join
# MAGIC **Description:** B DataFrame INNER JOIN A DataFrame UNION ROWS composed by COLUMNS from B DataFrame WITH NULL VALUES for COLUMNS in the A DataFrame WHERE CONDITION DOES NOT MATCH

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'right')) # synonyms: 'rightouter', 'right_outer'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC RIGHT JOIN dfB b -- synonyms: 'RIGHT OUTER JOIN'
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- | --- | -------- | -------- |
# MAGIC | 1 | abc | def | 1 | cbs | fed |
# MAGIC | 2 | 123 | 456 | 2 | 321 | 654 |
# MAGIC | 2 | 123 | 456 | 2 | dup321 | dup654 |
# MAGIC | 4 | null | null | 4 | ??? | !!! |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 6 | null | null | 6 | wtf | wow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full join
# MAGIC **Description:** A DataFrame LEFT JOIN B DataFrame UNION A DataFrame RIGHT JOIN B DataFrame 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'full')) # synonyms: 'outer', 'fullouter', 'full_outer'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC FULL JOIN dfB B  -- synonyms: 'FULL OUTER JOIN'
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- | --- | -------- | -------- |
# MAGIC | 1 | abc | def | 1 | cbs | fed |
# MAGIC | 2 | 123 | 456 | 2 | 321 | 654 |
# MAGIC | 2 | 123 | 456 | 2 | dup321 | dup654 |
# MAGIC | 3 | xxx | yyy | null | null | null |
# MAGIC | 3 | dupxxx | dupyyy | null | null | null |
# MAGIC | null | null | null | 4 | ??? | !!! |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | null | null | null | 6 | wtf | wow |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left anti join
# MAGIC **Description:** ROWS from A DataFrame WHERE CONDITION DOES NOT MATCH B DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'leftanti')) # synonyms: 'anti', 'left_anti'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC LEFT ANTI JOIN dfB B -- synonyms: 'ANTI JOIN'
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standard SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT A.* -- just columns from the A DataFrame
# MAGIC FROM dfA A
# MAGIC LEFT JOIN dfB B
# MAGIC ON A.key = B.key
# MAGIC WHERE B.key IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA |
# MAGIC | --- | -------- | -------- |
# MAGIC | 3 | xxx | yyy |
# MAGIC | 3 | dupxxx | dupyyy |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left semi join
# MAGIC **Description:** ROWS from A DataFrame WHERE CONDITION MATCHES B DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, dfA.key == dfB.key, how = 'leftsemi')) # synonyms: 'semi', 'left_semi'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC LEFT SEMI JOIN dfB B -- synonyms: 'SEMI JOIN'
# MAGIC ON A.key = B.key

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standard SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA A
# MAGIC WHERE EXISTS( -- If there is at least one key match in the B DataFrame
# MAGIC   SELECT B.*
# MAGIC   FROM dfB B
# MAGIC   WHERE A.key = B.key
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA |
# MAGIC | --- | -------- | -------- |
# MAGIC | 1 | abc | def |
# MAGIC | 2 | 123 | 456 |
# MAGIC | 5 | a1b1 | c2d2 |
# MAGIC | 5 | a1b1 | c2d2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross join
# MAGIC **Description:** ALL the possible COMBINATIONS of ROWS composed by ROWS from A DataFrame TOGETHER with ROWS from B DataFrame 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark version

# COMMAND ----------

display(dfA.join(dfB, how = 'cross'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA
# MAGIC CROSS JOIN dfB

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standard SQL version

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dfA
# MAGIC FULL JOIN dfB
# MAGIC ON TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC *Expected output:*
# MAGIC | key | col1_dfA | col2_dfA | key | col1_dfB | col2_dfB |
# MAGIC | --- | -------- | -------- | --- | -------- | -------- |
# MAGIC | 1 | abc | def | 1 | cbs | fed |
# MAGIC | 1 | abc | def | 2 | 321 | 654 |
# MAGIC | 1 | abc | def | 2 | dup321 | dup654 |
# MAGIC | 1 | abc | def | 4 | ??? | !!! |
# MAGIC | 1 | abc | def | 5 | 1b1a | 2d2c |
# MAGIC | 1 | abc | def | 6 | wtf | wow |
# MAGIC | 2 | 123 | 456 | 1 | cbs | fed |
# MAGIC | 3 | xxx | yyy | 1 | cbs | fed |
# MAGIC | 2 | 123 | 456 | 2 | 321 | 654 |
# MAGIC | 2 | 123 | 456 | 2 | dup321 | dup654 |
# MAGIC | 3 | xxx | yyy | 2 | 321 | 654 |
# MAGIC | 3 | xxx | yyy | 2 | dup321 | dup654 |
# MAGIC | 2 | 123 | 456 | 4 | ??? | !!! |
# MAGIC | 3 | xxx | yyy | 4 | ??? | !!! |
# MAGIC | 2 | 123 | 456 | 5 | 1b1a | 2d2c |
# MAGIC | 2 | 123 | 456 | 6 | wtf | wow |
# MAGIC | 3 | xxx | yyy | 5 | 1b1a | 2d2c | 
# MAGIC | 3 | xxx | yyy | 6 | wtf | wow |
# MAGIC | 3 | dupxxx | dupyyy | 1 | cbs | fed |
# MAGIC | 3 | dupxxx | dupyyy | 2 | 321 | 654 |
# MAGIC | 3 | dupxxx | dupyyy | 2 | dup321 | dup654 |
# MAGIC | 3 | dupxxx | dupyyy | 4 | ??? | !!! |
# MAGIC | 3 | dupxxx | dupyyy | 5 | 1b1a | 2d2c |
# MAGIC | 3 | dupxxx | dupyyy | 6 | wtf | wow |
# MAGIC | 5 | a1b1 | c2d2 | 1 | cbs | fed |
# MAGIC | 5 | a1b1 | c2d2 | 1 | cbs | fed |
# MAGIC | 5 | a1b1 | c2d2 | 2 | 321 | 654 |
# MAGIC | 5 | a1b1 | c2d2 | 2 | dup321 | dup654 |
# MAGIC | 5 | a1b1 | c2d2 | 2 | 321 | 654 |
# MAGIC | 5 | a1b1 | c2d2 | 2 | dup321 | dup654 |
# MAGIC | 5 | a1b1 | c2d2 | 4 | ??? | !!! |
# MAGIC | 5 | a1b1 | c2d2 | 4 | ??? | !!! |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 6 | wtf | wow |
# MAGIC | 5 | a1b1 | c2d2 | 5 | 1b1a | 2d2c |
# MAGIC | 5 | a1b1 | c2d2 | 6 | wtf | wow |
