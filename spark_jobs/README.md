#  Spark Jobs – SaaS Revenue Analytics

Esta carpeta contiene los scripts de procesamiento distribuido usando **PySpark** para construir el modelo dimensional del proyecto SaaS Revenue Analytics.

Incluye:

- Una **tabla de hechos**: `fact_revenue`
- Tres **tablas de dimensiones**: `dim_user`, `dim_subscription`, `dim_churn`

---

## 📄 Archivos

### `build_fact_table.py`

Este script realiza lo siguiente:

1. Inicializa una sesión de Spark.
2. Carga los datos limpios (`payments_clean.csv`).
3. Convierte `PaymentDate` a tipo fecha y crea una columna `Month`.
4. Construye la tabla de hechos `fact_revenue` con las siguientes columnas:
   - `UserID`
   - `PaymentDate`
   - `Amount`
   - `PaymentMethod`
   - `Month`
5. Guarda los resultados en la carpeta `data/fact_revenue/`.

---

### `build_dim_tables.py`

Este script genera las **tablas de dimensiones** a partir de los datasets limpios:

| Dimensión       | Origen CSV               | Columnas principales                                     |
|-----------------|--------------------------|----------------------------------------------------------|
| `dim_user`      | `users_clean.csv`        | `UserID`, `Country`, `RegistrationDate`                 |
| `dim_subscription` | `subscriptions_clean.csv` | `UserID`, `Plan`, `MonthlyPrice`, `DurationMonths`, `StartDate` |
| `dim_churn`     | `churn_clean.csv`        | `UserID`, `ChurnDate`, `ChurnReason`                    |

Se guardan como archivos CSV en las carpetas respectivas:

- `data/dim_user/`
- `data/dim_subscription/`
- `data/dim_churn/`

---

## 🧪 Cómo ejecutar los scripts

1. Activa tu entorno virtual.
2. Asegúrate de tener instalados `pyspark` y `findspark`.
3. Ejecuta cada archivo:

```bash
python build_fact_table.py
python build_dim_tables.py
