import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

FORMAT='%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO,
                    format=FORMAT,
                    handlers=[
                      logging.FileHandler("app_back_up.log",mode="a"),
                      logging.StreamHandler(sys.stdout)
                    ])

def back_up(p_table):
    logging.info("Respaldando {}".format(p_table))
    df = spark.read \
              .format("jdbc") \
              .option("driver","com.mysql.cj.jdbc.Driver") \
              .option("url", "jdbc:mysql://localhost:3306/globant") \
              .option("dbtable", p_table) \
              .option("user", "root") \
              .option("password", "Admin123") \
              .schema(d_tables[p_table]['schema']) \
              .load()

    df.write \
      .format('avro') \
      .mode('overwrite') \
      .save("./data/{}.avro".format(p_table))
    logging.info("Respaldo de {} ha concluido".format(p_table))

def restore(p_table):
    logging.info("Restaurando {}".format(p_table))
    df = spark.read \
              .format('avro') \
              .schema(d_tables[p_table]['schema']) \
              .load("./data/{}.avro".format(p_table))


    df.write \
      .format("jdbc") \
      .option("driver","com.mysql.cj.jdbc.Driver") \
      .option("url", "jdbc:mysql://localhost:3306/globant") \
      .option("dbtable", p_table) \
      .option("user", "root") \
      .option("password", "Admin123") \
      .option("truncate","true") \
      .mode("overwrite") \
      .save()
    logging.info("Restauracion de {} ha concluido".format(p_table))

if __name__ == "__main__":
    v_table = sys.argv[1]
    v_task = sys.argv[2]

    d_tables = { 'jobs': {'path': './jobs.csv', 
                         'schema': StructType([StructField('id',IntegerType(),True), 
                                               StructField('job',StringType(),True)])
                        },
                 'departments': {'path': './departments.csv',
                                 'schema': StructType([StructField('id',IntegerType(),True),
                                                       StructField('department',StringType(),True)])
                                },
                 'hired_employees': {'path': './hired_employees.csv',
                                     'schema': StructType([StructField('id',IntegerType(),True),
                                                           StructField('name',StringType(),True),
                                                           StructField('datetime',TimestampType(),True),
                                                           StructField('department_id',IntegerType(),True),
                                                           StructField('job_id',IntegerType(),True)])
                                    }
                }

    spark = SparkSession.builder.master("local[*]") \
                                .appName('prueba archivos') \
                                .config('spark.jars','mysql-connector-java-8.0.30.jar') \
                                .config('spark.jars.packages','org.apache.spark:spark-avro_2.12:3.3.1') \
                                .getOrCreate()
    if v_task == "back_up":
        back_up(v_table)
    elif v_task == "restore":
        restore(v_table)
