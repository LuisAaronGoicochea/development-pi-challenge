# Databricks notebook source
# Variables para la conexion al blob storage de azure datalake v2
v_account_key = dbutils.widgets.get("v_account_key")
v_account_name = dbutils.widgets.get("v_account_name")
v_source = dbutils.widgets.get("v_source")
v_mount_point = dbutils.widgets.get("v_mount_point")
v_file_path = dbutils.widgets.get("v_file_path")

# Variables para la configuración de la conexión a SQL Server
v_driver = dbutils.widgets.get("v_driver")
v_db_server = dbutils.widgets.get("v_db_server")
v_db_name = dbutils.widgets.get("v_db_name")
v_user = dbutils.widgets.get("v_user")
v_password = dbutils.widgets.get("v_password")
v_dbtable = dbutils.widgets.get("v_dbtable")
v_jdbc_url = f"jdbc:sqlserver://{v_db_server};databaseName={v_db_name};encrypt=true;trustServerCertificate=true;"

# Variables para la escritura del df a la tabla de sql server
v_mode = dbutils.widgets.get("v_mode")
v_format = dbutils.widgets.get("v_format")

# COMMAND ----------


def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

sub_unmount(v_mount_point)

# COMMAND ----------

dbutils.fs.mount(
    source = v_source,
    mount_point = v_mount_point,
    extra_configs = {v_account_name: v_account_key}
)

# COMMAND ----------

# Cargar el archivo CSV en un DataFrame
df = spark.read.csv(
    v_file_path, 
    header=True, 
    inferSchema=True
)

display(df)

# COMMAND ----------

# Agregar la columna FECHA_COPIA con la fecha actual
from pyspark.sql.functions import row_number, desc, current_timestamp, date_format
from pyspark.sql.window import Window

df = df.withColumn("FECHA_COPIA", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
display(df)

# COMMAND ----------

# Crear una ventana para numerar las filas por orden descendente de acuerdo a la columna FECHA_COPIA
windowSpec = Window().partitionBy("ID", "MUESTRA", "RESULTADO").orderBy(desc("FECHA_COPIA")) # Se tiene en cuenta, en este caso, que todos los registros fueron ingresados el mismo día.
df = df.withColumn("row_num", row_number().over(windowSpec))

display(df)

# COMMAND ----------

# Filtrar solo las filas con row_num igual a 1 (último registro por grupo)
df = df.filter("row_num = 1").drop("row_num")

display(df)

# COMMAND ----------

# Escribir el DataFrame en la tabla Unificado
df.write.format(v_format) \
    .option("driver", v_driver) \
    .option("url", v_jdbc_url) \
    .option("dbtable", v_dbtable) \
    .option("user", v_user) \
    .option("password", v_password) \
    .mode(v_mode) \
    .save()
