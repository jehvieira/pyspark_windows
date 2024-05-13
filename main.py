import pyspark
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]") \
 .appName('ProjectPro') \
 .getOrCreate()

data = [("James", "Sales", 3000), \
        ("Michael", "Sales", 4600), \
        ("Robert", "Sales", 4100), \
        ("Maria", "Finance", 3000), \
        ("James", "Sales", 3000), \
        ("Scott", "Finance", 3300), \
        ("Jen", "Finance", 3900), \
        ("Jeff", "Marketing", 3000), \
        ("Kumar", "Marketing", 2000), \
        ("Saif", "Sales", 4100), \
        ("Robert", "Sales", 4100) \
        ]

column= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = data, schema = column)
df.show()

spark.stop()
