import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

FORMAT='%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO,
                    format=FORMAT,
                    handlers=[
                      logging.FileHandler("app_load_history.log",mode="a"),
                      logging.StreamHandler(sys.stdout)
                    ])

if __name__ == "__main__":
    v_table = sys.argv[1]
    spark = SparkSession.builder.master("local[*]") \
                    .appName('carga historica') \
                    .config('spark.jars','mysql-connector-java-8.0.30.jar') \
                    .getOrCreate()

    d_tables = {'jobs': {'path': './jobs.csv', 
                         'schema': StructType([StructField('id',IntegerType(),False), 
                                               StructField('job',StringType(),False)])
                        },
                 'departments': {'path': './departments.csv',
                                 'schema': StructType([StructField('id',IntegerType(),False),
                                                       StructField('department',StringType(),False)])
                                },
                 'hired_employees': {'path': './hired_employees.csv',
                                     'schema': StructType([StructField('id',IntegerType(),False),
                                                           StructField('name',StringType(),False),
                                                           StructField('datetime',TimestampType(),False),
                                                           StructField('department_id',IntegerType(),False),
                                                           StructField('job_id',IntegerType(),False)])
                                    }
                }

    logging.info("Cargando {}".format(v_table))
    df = spark.read \
        .format('csv') \
        .option('header','false') \
        .schema(d_tables[v_table]['schema']) \
        .load(d_tables[v_table]['path'])

    if v_table == "jobs":
        df_null = df.filter(df.id.isNull() | df.job.isNull())
        df_not_null = df.filter(df.id.isNotNull() & df.job.isNotNull())
    elif v_table == "departments":
        df_null = df.filter(df.id.isNull() | df.department.isNull())
        df_not_null = df.filter(df.id.isNotNull() & df.department.isNotNull())
    elif v_table == "hired_employees":
        df_null = df.filter(df.id.isNull() | df.name.isNull() | df.datetime.isNull() | 
                            df.department_id.isNull() | df.job_id.isNull())
        df_not_null = df.filter(df.id.isNotNull() & df.name.isNotNull() & df.datetime.isNotNull() & 
                                df.department_id.isNotNull() & df.job_id.isNotNull())

    logging.warning("Id de filas con columnas en NULL \n{}".format([data[0] for data in df_null.select('id').collect()]))

    df_not_null.write \
      .format("jdbc") \
      .option("driver","com.mysql.cj.jdbc.Driver") \
      .option("url", "jdbc:mysql://localhost:3306/globant") \
      .option("dbtable", v_table) \
      .option("user", "root") \
      .option("password", "Admin123") \
      .option("truncate","true") \
      .mode("overwrite") \
      .save()

    logging.info("Se cargaron {} registros en la tabla {}".format(df_not_null.count(),v_table))