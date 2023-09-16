#!/usr/bin/env python
# coding: utf-8

# In[19]:


import findspark
findspark.init() #para inicializar
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder\
        .master("local[*]")\
        .appName('PySpark_training')\
        .getOrCreate() #devuelve una sesion existente sino existe la crea

spark = SparkSession.builder.getOrCreate() #genera lo mismo que antes solo que con los valores por default
sc = spark.sparkContext


# In[7]:


# 1. Crear un DataFrame con datos de personas, que incluya nombre, edad y ciudad de residencia.
person = [
    
        ("Alice", 25, "New York"),
        ("Bob", 30, "Los Angeles"),
        ("Charlie", 22, "Chicago")
      
]

df = spark.createDataFrame(person, ["name", "age", "city"])


# In[9]:


# 2. Mostrar solo los nombres de las personas del DataFrame.
selected_column = df.select("name")
selected_column.show()


# In[11]:


# 3. Filtrar personas cuya edad sea mayor o igual a 25.
filtered_column = df.filter(df["age"]>=25)
filtered_column.show()


# In[20]:


# 4. Agregar una nueva columna "Pais" con un valor constante para todas las filas.
country = "USA"
df_with_new_column = df.withColumn("country", lit(country) )
df_with_new_column.show()


# In[22]:


# 5. Calcular el promedio de edad de todas las personas.
average_age = df.select(avg(col("age"))).collect()[0][0]
print("Average Age:", average_age)


# In[23]:


# 6. Ordenar el DataFrame por edad en orden descendente.
df_ordered = df.orderBy(col("age").desc())
df_ordered.show()


# In[24]:


# 7. Agrupar por ciudad y calcular la cantidad de personas en cada ciudad.
result_group_personbycity = df.groupBy("city").agg(count("name").alias("total_persons"))
result_group_personbycity.show()


# In[27]:


# 8. Renombrar la columna "Nombre" a "NombreCompleto".
rename = df.withColumnRenamed("name", "CompleteName")
rename.show()


# In[28]:


# 9. Eliminar la columna "Edad" del DataFrame.
withnoage = df.drop("age")
withnoage.show()


# In[29]:


# 10. Realizar una consulta SQL en el DataFrame para seleccionar personas mayores de 20 años.
df.createOrReplaceTempView("my_table")
sql = "SELECT * FROM my_table WHERE age > 20"
queryresult = spark.sql(sql)
queryresult.show()


# In[30]:


# 11. Calcular la suma total de todas las edades.
total_sum = df.select(sum("age")).collect()[0][0]
print("Total Sum:", total_sum)


# In[31]:


# 12. Calcular la edad mínima y máxima de todas las personas.
max_age = df.select(max("age")).collect()[0][0]
min_age = df.select(min("age")).collect()[0][0]
print("Maximum Age:", max_age)
print("Minimum Age:", min_age)


# In[32]:


# 13. Filtrar personas cuya ciudad de residencia sea "Chicago" y edad sea menor de 30.
df.createOrReplaceTempView("my_table")
sql = "SELECT * FROM my_table WHERE city = 'Chicago' AND age < 30"
queryresult = spark.sql(sql)
queryresult.show()


# In[33]:


# 14. Agregar una nueva columna "EdadDuplicada" que contenga el doble de la edad.
duplicatedage = df.withColumn("DuplicatedAge", col("age") * 2)
duplicatedage.show()


# In[42]:


# 15. Convertir todas las edades en años a meses.
agecolumn = "age"
ageInMonths = df.withColumn("age", expr(f"{agecolumn} * 12"))
ageInMonths.show()


# In[44]:


# 16. Contar el número total de personas en el DataFrame.
person_count = df.select("name").count()
print("Total Number of Persons:", person_count)


# In[48]:


# 17. Filtrar personas cuya edad sea un número par.
evenAges = df.filter(col("age") % 2 == 0)
evenAges.show()


# In[49]:


# 18. Calcular la cantidad de personas por rango de edades (0-20, 21-40, 41-60, 61+).
age_column = "age"
df_age_ranges = df.select(
    count(when((col(age_column) >= 0) & (col(age_column) <= 20), 1)).alias("0-20"),
    count(when((col(age_column) >= 21) & (col(age_column) <= 40), 1)).alias("21-40"),
    count(when((col(age_column) >= 41) & (col(age_column) <= 60), 1)).alias("41-60"),
    count(when(col(age_column) >= 61, 1)).alias("61+")
)
df_age_ranges.show()


# In[50]:


# 19. Contar cuántas personas tienen el mismo nombre.
name_counts = df.groupBy("name").agg(count("*").alias("count"))
name_counts.show()


# In[51]:


# 20. Concatenar las columnas "Nombre" y "Ciudad" en una nueva columna llamada "InformacionPersonal".
concatenated = df.withColumn("PersonalInfo", concat_ws(", ", col("name"), col("city")))
concatenated.show()


# In[ ]:




