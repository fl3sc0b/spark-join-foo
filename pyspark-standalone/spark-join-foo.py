# spark-join-foo
# A set of examples for each of the seven types of joins in spark
# Pyspark standalone code version

from pyspark.sql import SparkSession

from pyspark.sql.types import *

spark = SparkSession.builder.appName("spark-join-foo").getOrCreate()

#################
## Dataframe A ##
#################

print("""
#################
## Dataframe A ##
#################
""")

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

dfA.show()

# Expected output:
# +---+--------+--------+                                                      
# |key|col1_dfA|col2_dfA|
# +---+--------+--------+
# |  1|     abc|     def|
# |  2|     123|     456|
# |  3|     xxx|     yyy|
# |  3|  dupxxx|  dupyyy|
# |  5|    a1b1|    c2d2|
# |  5|    a1b1|    c2d2|
# +---+--------+--------+

#################
## Dataframe B ##
#################

print("""
#################
## Dataframe B ##
#################
""")

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

dfB.show()

# Expected output:
# +---+--------+--------+
# |key|col1_dfB|col2_dfB|
# +---+--------+--------+
# |  1|     cbs|     fed|
# |  2|     321|     654|
# |  2|  dup321|  dup654|
# |  4|     ???|     !!!|
# |  5|    1b1a|    2d2c|
# |  6|     wtf|     wow|
# +---+--------+--------+

################
## Inner join ##
############################################################################################
## Description: ROWS composed by COLUMNS from BOTH DataFrames AND WHERE CONDITION MATCHES ##
############################################################################################

print("""
################
## Inner join ##
################
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'inner').show()

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
INNER JOIN dfB B
ON A.key = B.key
""").show()

# Expected output:
# +---+--------+--------+---+--------+--------+                             
# |key|col1_dfA|col2_dfA|key|col1_dfB|col2_dfB|
# +---+--------+--------+---+--------+--------+
# |  1|     abc|     def|  1|     cbs|     fed|
# |  2|     123|     456|  2|     321|     654|
# |  2|     123|     456|  2|  dup321|  dup654|
# |  5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# |  5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# +---+--------+--------+---+--------+--------+

###############
## Left join ##
#####################################################################################
## Description: A DataFrame INNER JOIN B DataFrame UNION ROWS composed by COLUMNS  ##
# from A DataFrame WITH NULL VALUES for COLUMNS in the B DataFrame WHERE CONDITION ##
# DOES NOT MATCH                                                                   ##
#####################################################################################

print("""
###############
## Left join ##
###############
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'left').show() # synonyms: 'leftouter', 'left_outer'

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
LEFT JOIN dfB B -- synonyms: 'LEFT OUTER JOIN'
ON A.key = B.key
""").show()

# Expected output:
# +---+--------+--------+----+--------+--------+
# |key|col1_dfA|col2_dfA| key|col1_dfB|col2_dfB|
# +---+--------+--------+----+--------+--------+
# |  1|     abc|     def|   1|     cbs|     fed|
# |  2|     123|     456|   2|     321|     654|
# |  2|     123|     456|   2|  dup321|  dup654|
# |  3|     xxx|     yyy|null|    null|    null|
# |  3|  dupxxx|  dupyyy|null|    null|    null|
# |  5|    a1b1|    c2d2|   5|    1b1a|    2d2c|
# |  5|    a1b1|    c2d2|   5|    1b1a|    2d2c|
# +---+--------+--------+----+--------+--------+

################
## Right join ##
#####################################################################################
## Description: B DataFrame INNER JOIN A DataFrame UNION ROWS composed by COLUMNS  ##
# from B DataFrame WITH NULL VALUES for COLUMNS in the A DataFrame WHERE CONDITION ##
# DOES NOT MATCH                                                                   ##
#####################################################################################

print("""
################
## Right join ##
################
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'right').show() # synonyms: 'rightouter', 'right_outer'

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
RIGHT JOIN dfB b -- synonyms: 'RIGHT OUTER JOIN'
ON A.key = B.key
""").show()

# # Expected output:
# +----+--------+--------+---+--------+--------+
# |   1|     abc|     def|  1|     cbs|     fed|
# |   2|     123|     456|  2|     321|     654|
# |   2|     123|     456|  2|  dup321|  dup654|
# |null|    null|    null|  4|     ???|     !!!|
# |   5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# |   5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# |null|    null|    null|  6|     wtf|     wow|
# +----+--------+--------+---+--------+--------+

################
## Full join ##
#############################################################################################
## Description: A DataFrame LEFT JOIN B DataFrame UNION A DataFrame RIGHT JOIN B DataFrame ##
#############################################################################################

print("""
###############
## Full join ##
###############
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'full').show() # synonyms: 'outer', 'fullouter', 'full_outer'

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
FULL JOIN dfB B  -- synonyms: 'FULL OUTER JOIN'
ON A.key = B.key
""").show()

# # Expected output:
# +----+--------+--------+----+--------+--------+
# |   1|     abc|     def|   1|     cbs|     fed|
# |   2|     123|     456|   2|     321|     654|
# |   2|     123|     456|   2|  dup321|  dup654|
# |   3|     xxx|     yyy|null|    null|    null|
# |   3|  dupxxx|  dupyyy|null|    null|    null|
# |null|    null|    null|   4|     ???|     !!!|
# |   5|    a1b1|    c2d2|   5|    1b1a|    2d2c|
# |   5|    a1b1|    c2d2|   5|    1b1a|    2d2c|
# |null|    null|    null|   6|     wtf|     wow|
# +----+--------+--------+----+--------+--------+

####################
## Left anti join ##
###################################################################################
## Description: ROWS from A DataFrame WHERE CONDITION DOES NOT MATCH B DataFrame ##
###################################################################################

print("""
####################
## Left anti join ##
####################
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'leftanti').show() # synonyms: 'anti', 'left_anti'

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
LEFT ANTI JOIN dfB B -- synonyms: 'ANTI JOIN'
ON A.key = B.key
""").show()

# Standard SQL version
print("Standard SQL version:")
spark.sql("""
SELECT A.* -- just columns from the A DataFrame
FROM dfA A
LEFT JOIN dfB B
ON A.key = B.key
WHERE B.key IS NULL
""").show()

# Expected output:
# +---+--------+--------+
# |key|col1_dfA|col2_dfA|
# +---+--------+--------+
# |  3|     xxx|     yyy|
# |  3|  dupxxx|  dupyyy|
# +---+--------+--------+

####################
## Left semi join ##
############################################################################
## Description: ROWS from A DataFrame WHERE CONDITION MATCHES B DataFrame ##
############################################################################

print("""
####################
## Left semi join ##
####################
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, dfA.key == dfB.key, how = 'leftsemi').show() # synonyms: 'semi', 'left_semi'

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA A
LEFT SEMI JOIN dfB B -- synonyms: 'SEMI JOIN'
ON A.key = B.key
""").show()

# Standard SQL version
print("Standard SQL version:")
spark.sql("""
SELECT *
FROM dfA A
WHERE EXISTS( -- If there is at least one key match in the B DataFrame
  SELECT B.*
  FROM dfB B
  WHERE A.key = B.key
)
""").show()

# Expected output:
# +---+--------+--------+
# |key|col1_dfA|col2_dfA|
# +---+--------+--------+
# |  1|     abc|     def|
# |  2|     123|     456|
# |  5|    a1b1|    c2d2|
# |  5|    a1b1|    c2d2|
# +---+--------+--------+

################
## Cross join ##
##########################################################################################
## Description: ALL the possible COMBINATIONS of ROWS composed by ROWS from A DataFrame ##
## TOGETHER with ROWS from B DataFrame                                                  ##
##########################################################################################

print("""
################
## Cross join ##
################
""")

# Pyspark version
print("Pyspark version:")
dfA.join(dfB, how = 'cross').show(36)

# Spark SQL version
print("Spark SQL version:")
spark.sql("""
SELECT *
FROM dfA
CROSS JOIN dfB
""").show(36)

# Standard SQL version
print("Standard SQL version:")
spark.sql("""
SELECT *
FROM dfA
FULL JOIN dfB
ON TRUE
""").show(36)

# Expected output:
# +---+--------+--------+---+--------+--------+                                   
# |key|col1_dfA|col2_dfA|key|col1_dfB|col2_dfB|
# +---+--------+--------+---+--------+--------+
# |  1|     abc|     def|  1|     cbs|     fed|
# |  1|     abc|     def|  2|     321|     654|
# |  1|     abc|     def|  2|  dup321|  dup654|
# |  1|     abc|     def|  4|     ???|     !!!|
# |  1|     abc|     def|  5|    1b1a|    2d2c|
# |  1|     abc|     def|  6|     wtf|     wow|
# |  2|     123|     456|  1|     cbs|     fed|
# |  2|     123|     456|  2|     321|     654|
# |  2|     123|     456|  2|  dup321|  dup654|
# |  2|     123|     456|  4|     ???|     !!!|
# |  2|     123|     456|  5|    1b1a|    2d2c|
# |  2|     123|     456|  6|     wtf|     wow|
# |  3|     xxx|     yyy|  1|     cbs|     fed|
# |  3|     xxx|     yyy|  2|     321|     654|
# |  3|     xxx|     yyy|  2|  dup321|  dup654|
# |  3|     xxx|     yyy|  4|     ???|     !!!|
# |  3|     xxx|     yyy|  5|    1b1a|    2d2c|
# |  3|     xxx|     yyy|  6|     wtf|     wow|
# |  3|  dupxxx|  dupyyy|  1|     cbs|     fed|
# |  3|  dupxxx|  dupyyy|  2|     321|     654|
# |  3|  dupxxx|  dupyyy|  2|  dup321|  dup654|
# |  3|  dupxxx|  dupyyy|  4|     ???|     !!!|
# |  3|  dupxxx|  dupyyy|  5|    1b1a|    2d2c|
# |  3|  dupxxx|  dupyyy|  6|     wtf|     wow|
# |  5|    a1b1|    c2d2|  1|     cbs|     fed|
# |  5|    a1b1|    c2d2|  2|     321|     654|
# |  5|    a1b1|    c2d2|  2|  dup321|  dup654|
# |  5|    a1b1|    c2d2|  4|     ???|     !!!|
# |  5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# |  5|    a1b1|    c2d2|  6|     wtf|     wow|
# |  5|    a1b1|    c2d2|  1|     cbs|     fed|
# |  5|    a1b1|    c2d2|  2|     321|     654|
# |  5|    a1b1|    c2d2|  2|  dup321|  dup654|
# |  5|    a1b1|    c2d2|  4|     ???|     !!!|
# |  5|    a1b1|    c2d2|  5|    1b1a|    2d2c|
# |  5|    a1b1|    c2d2|  6|     wtf|     wow|
# +---+--------+--------+---+--------+--------+
