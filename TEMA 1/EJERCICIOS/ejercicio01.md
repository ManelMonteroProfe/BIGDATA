# Ejercicio 01 — Introducción a PySpark: Análisis de Ventas
**Curso:** IFCT0022 — Gestión de Big Data  
**Nivel:** Iniciación  
**Duración estimada:** 2 horas  
**Herramienta:** PySpark + Google Colab (sin instalación)

---

## Objetivos del ejercicio

Al finalizar este ejercicio, el alumno será capaz de:

- Crear una sesión de Spark y entender su arquitectura básica
- Cargar datos desde un fichero CSV con inferencia de esquema
- Realizar operaciones de transformación: filtrado, agrupación y ordenación
- Comparar el enfoque de Pandas (escala pequeña) con PySpark (gran escala)
- Interpretar y visualizar los resultados obtenidos

---

## Contexto

Somos el equipo de datos de una empresa distribuidora con tiendas en varias ciudades de España. Tenemos un fichero de transacciones de ventas del último año y necesitamos analizarlo para detectar qué categorías de producto generan más ingresos y cuáles están por debajo del umbral de rentabilidad.

---

## Parte 0 — Preparación del entorno

### 0.1 Abrir Google Colab

1. Ir a [https://colab.research.google.com](https://colab.research.google.com)
2. Crear un nuevo notebook: **Archivo → Nuevo cuaderno**
3. Renombrarlo como `ejercicio01_pyspark_ventas`

### 0.2 Instalar PySpark en Colab

En la primera celda del notebook, ejecutar:

```python
# Instalación de PySpark (solo necesario en Colab)
!pip install pyspark -q
```

> **Nota:** En un entorno corporativo o clúster real, PySpark ya estaría instalado y este paso no sería necesario.

---

## Parte 1 — Generar el dataset de ejemplo

En Big Data trabajamos con ficheros grandes. Para este ejercicio vamos a **generar un CSV sintético** de 100.000 registros de ventas.

```python
import pandas as pd
import numpy as np
import random

# Fijamos semilla para reproducibilidad
np.random.seed(42)
random.seed(42)

# Parámetros del dataset
n_registros = 100_000

categorias  = ["Electrónica", "Ropa", "Alimentación", "Hogar", "Deportes", "Juguetes"]
ciudades    = ["Madrid", "Barcelona", "Sevilla", "Valencia", "Bilbao", "Zaragoza"]
meses       = list(range(1, 13))

# Generación de datos
data = {
    "id_venta":   range(1, n_registros + 1),
    "fecha":      pd.date_range("2024-01-01", periods=n_registros, freq="1min").strftime("%Y-%m-%d"),
    "ciudad":     [random.choice(ciudades) for _ in range(n_registros)],
    "categoria":  [random.choice(categorias) for _ in range(n_registros)],
    "producto":   [f"PROD-{random.randint(1000, 9999)}" for _ in range(n_registros)],
    "cantidad":   np.random.randint(1, 20, size=n_registros),
    "precio_unit":np.round(np.random.uniform(5.0, 500.0, size=n_registros), 2),
}

df_pandas = pd.DataFrame(data)
df_pandas["importe"] = np.round(df_pandas["cantidad"] * df_pandas["precio_unit"], 2)

# Guardar como CSV
df_pandas.to_csv("ventas.csv", index=False)

print(f"✅ Dataset generado: {n_registros:,} registros")
print(df_pandas.head(5))
```

**Resultado esperado:** un fichero `ventas.csv` con 100.000 filas y las columnas:  
`id_venta`, `fecha`, `ciudad`, `categoria`, `producto`, `cantidad`, `precio_unit`, `importe`

---

## Parte 2 — Crear la sesión de Spark

```python
from pyspark.sql import SparkSession

# Creamos la sesión (punto de entrada a Spark)
spark = SparkSession.builder \
    .appName("Análisis de Ventas - IFCT0022") \
    .getOrCreate()

# Comprobamos la versión
print(f"✅ Spark version: {spark.version}")
```

> **¿Qué es una SparkSession?**  
> Es el objeto principal desde el que controlamos todo Spark. En un clúster real, este objeto gestiona la conexión con el nodo maestro (driver) y los nodos de trabajo (workers).

---

## Parte 3 — Cargar el CSV con Spark

```python
# Cargamos el CSV con inferencia automática de tipos
df = spark.read.csv(
    "ventas.csv",
    header=True,
    inferSchema=True
)

# Exploramos el esquema
print("📋 Esquema del DataFrame:")
df.printSchema()

# Primeras filas
print("\n📄 Muestra de datos:")
df.show(5, truncate=False)

# Número total de registros
print(f"\n🔢 Total de registros: {df.count():,}")
```

**Resultado esperado:**

```
📋 Esquema del DataFrame:
root
 |-- id_venta: integer
 |-- fecha: string
 |-- ciudad: string
 |-- categoria: string
 |-- producto: string
 |-- cantidad: integer
 |-- precio_unit: double
 |-- importe: double

🔢 Total de registros: 100,000
```

> **Diferencia clave con Pandas:**  
> En Pandas, `pd.read_csv()` carga todo en memoria RAM. En Spark, los datos se distribuyen en particiones entre los workers y **no se cargan hasta que se ejecuta una acción** (como `show()` o `count()`). Esto se llama **evaluación perezosa (lazy evaluation)**.

---

## Parte 4 — Transformaciones básicas

### 4.1 Filtrado de filas

```python
from pyspark.sql.functions import col

# Ventas con importe superior a 1.000 €
ventas_altas = df.filter(col("importe") > 1000)

print(f"Ventas con importe > 1.000€: {ventas_altas.count():,}")
ventas_altas.show(5)
```

### 4.2 Selección de columnas

```python
# Seleccionamos solo las columnas relevantes
df_reducido = df.select("fecha", "ciudad", "categoria", "importe")
df_reducido.show(5)
```

### 4.3 Estadísticas descriptivas

```python
# Estadísticas básicas del importe
print("📊 Estadísticas descriptivas:")
df.select("importe", "cantidad", "precio_unit").describe().show()
```

---

## Parte 5 — Agrupación y agregaciones

```python
from pyspark.sql.functions import sum, count, avg, round

# Agrupamos por categoría
resumen_categoria = df.groupBy("categoria").agg(
    sum("importe").alias("total_ventas"),
    count("id_venta").alias("num_transacciones"),
    round(avg("importe"), 2).alias("ticket_medio")
).orderBy(col("total_ventas").desc())

print("📊 Resumen de ventas por categoría:")
resumen_categoria.show()
```

**Resultado esperado (aproximado):**

```
+------------+------------------+-----------------+------------+
|  categoria | total_ventas     |num_transacciones|ticket_medio|
+------------+------------------+-----------------+------------+
|Electrónica |  4,235,891.45    |      16,712      |  253.45    |
|Hogar       |  4,198,234.12    |      16,680      |  251.69    |
|Deportes    |  4,175,003.78    |      16,659      |  250.62    |
|...         |  ...             |       ...        |  ...       |
+------------+------------------+-----------------+------------+
```

---

## Parte 6 — Filtrar categorías por umbral de rentabilidad

```python
# Categorías con ventas totales superiores a 4.000.000 €
categorias_rentables = resumen_categoria.filter(
    col("total_ventas") > 4_000_000
)

print("✅ Categorías que superan el umbral de rentabilidad (> 4M €):")
categorias_rentables.show()
```

---

## Parte 7 — Análisis por ciudad

```python
# Top 3 ciudades por ingresos totales
top_ciudades = df.groupBy("ciudad").agg(
    sum("importe").alias("total_ventas")
).orderBy(col("total_ventas").desc()).limit(3)

print("🏆 Top 3 ciudades por ingresos:")
top_ciudades.show()
```

---

## Parte 8 — Guardar los resultados

```python
# Guardamos el resumen como CSV
resumen_categoria.coalesce(1).write.csv(
    "resultado_categorias",
    header=True,
    mode="overwrite"
)

print("💾 Resultados guardados en la carpeta 'resultado_categorias'")
```

> **¿Qué es `coalesce(1)`?**  
> Spark divide los datos en particiones para procesarlos en paralelo. `coalesce(1)` une todas las particiones en un solo fichero de salida. En producción, con millones de registros, se suelen mantener varias particiones.

---

## Parte 9 — Comparativa Pandas vs PySpark

```python
import time

# ⏱️ Tiempo con Pandas
inicio = time.time()
df_p = pd.read_csv("ventas.csv")
resultado_pandas = df_p.groupby("categoria")["importe"].sum().reset_index()
fin = time.time()
print(f"Pandas:  {fin - inicio:.3f} segundos")

# ⏱️ Tiempo con PySpark
inicio = time.time()
resultado_spark = df.groupBy("categoria").agg(sum("importe")).collect()
fin = time.time()
print(f"PySpark: {fin - inicio:.3f} segundos")

print("\n⚠️  Con 100K registros en local, Pandas puede ser más rápido.")
print("    PySpark destaca con cientos de millones de registros en clúster.")
```

> **Reflexión importante para los alumnos:**  
> PySpark introduce overhead (tiempo de arranque, serialización, etc.) que en datasets pequeños hace que sea más lento que Pandas. Su ventaja aparece cuando los datos **no caben en la RAM de una sola máquina** o cuando se ejecuta en un clúster con múltiples nodos.

---

## Parte 10 — Cerrar la sesión de Spark

```python
# Buena práctica: liberar recursos al finalizar
spark.stop()
print("🔴 Sesión de Spark cerrada")
```

---

## Resumen de operaciones aprendidas

| Operación | Método PySpark |
|---|---|
| Cargar CSV | `spark.read.csv(...)` |
| Ver esquema | `df.printSchema()` |
| Contar registros | `df.count()` |
| Filtrar filas | `df.filter(col(...))` |
| Seleccionar columnas | `df.select(...)` |
| Agrupar y agregar | `df.groupBy(...).agg(...)` |
| Ordenar | `df.orderBy(...)` |
| Guardar resultados | `df.write.csv(...)` |

---

## Ejercicio adicional (para alumnos avanzados)

Añade al análisis las siguientes consultas:

1. **¿Qué ciudad tiene el ticket medio más alto?**
2. **¿Cuál es el mes con más ventas del año?** *(pista: usa `month()` de `pyspark.sql.functions`)*
3. **¿Qué combinación `ciudad + categoría` genera más ingresos?**

---

## Referencias

- [Documentación oficial de PySpark](https://spark.apache.org/docs/latest/api/python/)
- [Google Colab — Getting Started](https://colab.research.google.com/notebooks/intro.ipynb)
- [Dataset de ejemplo en Kaggle](https://www.kaggle.com/datasets)

---

*Ejercicio elaborado para el curso IFCT0022 — Gestión de Big Data*
