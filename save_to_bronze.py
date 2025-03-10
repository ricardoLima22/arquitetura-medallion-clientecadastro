from pyspark.sql import SparkSession
import pyspark.sql.functions as F

    ##ATUCAD
class atucad_to_load:
    def __init__(self,spark):
        self.spark = spark

    def processing_atucad_bronze(self,path_read,path_write):
        try:
            df_atucad = self.spark.read\
                            .format("csv")\
                            .option("header" , True)\
                            .option("inferSchema", True)\
                            .option("sep", ",")\
                            .load(path_read)

            df_atucad.write.format("parquet")\
                            .mode("overwrite")\
                            .save(path_write)

        except Exception:
            print("erro")
    
            return True

##CUSTOMEREXP
class customerexp_to_load:
    def __init__(self,spark):
        self.spark = spark
    
    def processing_customerexp_bronze(self,path_read,path_write):

        df_customerexp = self.spark.read\
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
class portal_cliente_to_load:
    def __init__(self,spark):
        self.spark = spark

    def processing_portalcliente_bronze(self,path_read,path_write):

        df_portalcliente = self.spark.read\
                                .format("csv")\
                                .option("header" , True)\
                                .option("inferSchema", True)\
                                .option("sep", ",")\
                                .load(path_read)

        df_portalcliente.write.format("parquet")\
                              .mode("overwrite")\
                              .save(path_write)

        return True

