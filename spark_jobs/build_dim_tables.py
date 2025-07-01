import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Create Dimension Tables") \
    .getOrCreate()

# Leer los archivos limpios
df_users = spark.read.option("header", True).csv("../data/users_clean.csv")
df_subs = spark.read.option("header", True).csv("../data/subscriptions_clean.csv")
df_churn = spark.read.option("header", True).csv("../data/churn_clean.csv")

# Convertir columnas de fecha al tipo fecha
df_users = df_users.withColumn("RegistrationDate", to_date(col("RegistrationDate"), "yyyy-MM-dd"))
df_subs = df_subs.withColumn("StartDate", to_date(col("StartDate"), "yyyy-MM-dd"))
df_churn = df_churn.withColumn("ChurnDate", to_date(col("ChurnDate"), "yyyy-MM-dd"))

# Crear tablas de dimensión
dim_user = df_users.select("UserID", "Country", "RegistrationDate")
dim_subscription = df_subs.select("UserID", "Plan", "MonthlyPrice", "DurationMonths", "StartDate")
dim_churn = df_churn.select("UserID", "ChurnDate", "ChurnReason")

# Guardar como CSV (en una carpeta por tabla)
dim_user.write.mode("overwrite").option("header", True).csv("../data/dim_user")
dim_subscription.write.mode("overwrite").option("header", True).csv("../data/dim_subscription")
dim_churn.write.mode("overwrite").option("header", True).csv("../data/dim_churn")

print("✅ Tablas de dimensiones creadas correctamente.")
