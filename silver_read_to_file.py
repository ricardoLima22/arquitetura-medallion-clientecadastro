from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class readSilver:

    def __init__(self,spark):
        self.spark = spark


    def silver_read(self,path):

        try:
            df_Teste = self.spark.read\
                                 .format("parquet")\
                                 .load(path)

            df_Teste.show()

            return df_Teste


        except Exception as e:
            print("Arquivo não encontrado")     
        return None
    
class silver_transformation:

    def __init__(self,spark):
        self.spark = spark

    def teste (self, df):

        df.select(F.col("cpf")).show()

        return df

