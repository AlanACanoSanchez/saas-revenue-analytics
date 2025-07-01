import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.sql.functions import to_date, date_format

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

# Convertir fechas
payments = payments.withColumn("PaymentDate", to_date(col("PaymentDate"), "yyyy-MM-dd"))
payments = payments.withColumn("Month", date_format(col("PaymentDate"), "yyyy-MM"))

# Crear tabla de hechos
fact_revenue = payments.select(
    col("UserID"),
    col("PaymentDate"),
    col("Amount"),
    col("PaymentMethod"),
    col("Month")
)

# Guardar como CSV limpio
fact_revenue.write.mode("overwrite").option("header", True).csv("../data/fact_revenue")
print("✅ Tabla de hechos generada correctamente")
