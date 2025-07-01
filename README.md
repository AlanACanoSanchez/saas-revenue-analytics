# 📊 SaaS Revenue Analytics

Proyecto de análisis de ingresos y cancelaciones en una empresa SaaS utilizando **PySpark**, **Python**, **Power BI** y un enfoque orientado a datos.

Incluye limpieza de datos, construcción de un modelo dimensional (esquema estrella), integración con PySpark y visualización futura con herramientas BI.

---

## 🎯 Objetivo

- Analizar los ingresos mensuales generados por usuarios.
- Identificar tasas de cancelación (churn) y razones más frecuentes.
- Detectar patrones por país, tipo de suscripción y duración del plan.
- Implementar un flujo ETL escalable con PySpark.
- Construir una **tabla de hechos** para análisis avanzado en Power BI.

---

## 🛠️ Tecnologías utilizadas

- **Python** (pandas, matplotlib)
- **PySpark** (procesamiento distribuido)
- **Power BI** (visualización futura)
- **SQL** (consultas exploratorias)
- **Jupyter Notebooks**
- **Git & GitHub**

---

## 🧱 Modelo Dimensional

Se implementó un esquema tipo **estrella** compuesto por:

### 📌 Tabla de Hechos – `fact_revenue`

| Campo         | Descripción                          |
|---------------|--------------------------------------|
| UserID        | Usuario que realiza el pago          |
| PaymentDate   | Fecha del pago                       |
| Amount        | Monto pagado en USD                  |
| PaymentMethod | Método de pago                       |
| Month         | Mes del pago (YYYY-MM)               |

---

### 📂 Tablas de Dimensión

#### `dim_user`
- `UserID`  
- `Country`  
- `RegistrationDate`  

#### `dim_subscription`
- `UserID`  
- `Plan` (Basic, Standard, Premium)  
- `MonthlyPrice`  
- `DurationMonths`  
- `StartDate`  

#### `dim_churn`
- `UserID`  
- `ChurnDate`  
- `ChurnReason`  

> *Posible extensión futura: `dim_date` para análisis temporal más detallado.*

---

## 📁 Estructura del proyecto

saas-revenue-analytics/
│
├── data/ # Datos originales y limpios (CSV)
├── notebooks/ # Limpieza y análisis exploratorio
├── spark_jobs/ # Scripts PySpark para construir tabla de hechos
├── sql/ # Consultas y transformaciones SQL
├── dashboards/ # Power BI (próximamente)
├── dags/ # Automatización futura con Airflow
├── README.md # Documentación principal del proyecto
├── requirements.txt # Librerías necesarias
└── .gitignore


---

## ⚙️ Cómo reproducir el proyecto

1. Clona el repositorio
2. Crea y activa un entorno virtual
3. Instala dependencias: `pip install -r requirements.txt`
4. Ejecuta los notebooks para explorar y limpiar los datos (notebooks/exploracion_inicial.ipynb)
5. Ejecuta la construcción de la tabla de hechos con Spark (python spark_jobs/build_fact_table.py)
6. Carga el resultado en Power BI para análisis interactivo


## 📈 Dashboard 

Dashboard interactivo construido en Power BI con indicadores clave.

![Vista general del dashboard]()

## 🧠 Observaciones clave del análisis

- 

## 👤 Autor

- [Alan Arturo Cano Sanchez](https://www.linkedin.com/in/alan-arturo-cano-sanchez-511855361)
- Egresado de Ingeniería en Datos e Inteligencia Organizacional
