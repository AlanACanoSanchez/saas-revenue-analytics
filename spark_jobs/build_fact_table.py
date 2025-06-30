import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("SaaS Revenue Analytics") \
    .getOrCreate()

# Leer los datasets limpios
users = spark.read.option("header", True).csv("../data/users_clean.csv")
subs = spark.read.option("header", True).csv("../data/subscriptions_clean.csv")
payments = spark.read.option("header", True).csv("../data/payments_clean.csv")
churn = spark.read.option("header", True).csv("../data/churn_clean.csv")

# Probar
print("✅ SparkSession creada correctamente:", spark.version)
users.show(5)
