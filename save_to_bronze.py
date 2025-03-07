from pyspark.sql import SparkSession
import pyspark.sql.functions as F

    ##ATUCAD
def processing_atucad_bronze(path_read,path_write, spark):

    df_atucad = spark.read\
                   .format("csv")\
                   .option("header" , True)\
                   .option("inferSchema", True)\
                   .option("sep", ",")\
                   .load(path_read)

    df_atucad.write.format("parquet")\
                    .mode("overwrite")\
                    .save(path_write)

    return True


    ##CUSTOMEREXP
def processing_customerexp_bronze(path_read,path_write,spark):

    df_customerexp = spark.read\
                   .format("csv")\
                   .option("header" , True)\
                   .option("inferSchema", True)\
                   .option("sep", ",")\
                   .load(path_read)

    df_customerexp.write.format("parquet")\
                        .mode("overwrite")\
                        .save(path_write)

    return True   


    ##PORTALCLIENTE

def processing_portalcliente_bronze(path_read,path_write, spark):

    df_portalcliente = spark.read\
                   .format("csv")\
                   .option("header" , True)\
                   .option("inferSchema", True)\
                   .option("sep", ",")\
                   .load(path_read)

    df_portalcliente.write.format("parquet")\
                          .mode("overwrite")\
                          .save(path_write)

    return True

