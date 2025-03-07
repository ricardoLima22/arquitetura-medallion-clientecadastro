from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
    ##ATUCAD
def processing_atucad(path, spark):

    df_atucad = spark.read\
                   .format("parquet")\
                   .load(path)

    df_select = (
        df_atucad.select(
                F.col("crmclia_cpf").alias("cpf"),
                F.col("crmclia_ender_cep").alias("cep"),
                F.col("crmclia_ender_complemento").alias("complemento"),
                F.col("crmclia_ender_logradouro").alias("logradouro"),
                F.col("crmclia_ender_cidade").alias("cidade"),
                F.col("crmclia_dtins").alias("data_alteracao")
        )
    )


    df_new = (
      df_select.withColumn("tipo_logradouro", F.lit(None).cast("string"))
                .withColumn("bairro", F.lit(None).cast("string"))
                .withColumn("uf", F.lit(None).cast("string"))
                
    )   


    return df_new


    ##CUSTOMEREXP
def processing_customerexp(path,spark):

    df_customerexp = spark.read\
                    .format("parquet")\
                    .load(path)


    df_select1 = (
        df_customerexp
        .select(
                F.col("customer_cpf").alias("cpf"),
                F.col("postal_code").alias("cep"),
                F.col("address_complement").alias("complemento"),
                F.col("address").alias("logradouro"),
                F.col("town").alias("cidade"),
                F.col("region").alias("uf"),
                F.col("regsitration_date").alias("data_alteracao")
        )
    )


    df_new = (
        df_select1  
                .withColumn("tipo_logradouro", F.lit(None).cast("string"))
                .withColumn("bairro", F.lit(None).cast("string"))
    ).select("cpf","cep", "complemento", "logradouro", "cidade", "data_alteracao","tipo_logradouro","bairro","uf")


    return df_new   

    ##PORTALCLIENTE
def processing_portalcliente(path, spark):

    df_portalcliente = spark.read\
                    .format("parquet")\
                    .load(path)

    df_select1 = (
        df_portalcliente
        .select(
                F.col("clien_cnpjcpf").alias("cpf"),
                F.col("clien_cep").alias("cep"),
                F.col("clien_endereco").alias("logradouro"),
                F.col("clien_complemento").alias("complemento"),
                F.col("clien_cidade").alias("cidade"),
                F.col("clien_uf").alias("uf"),
                F.col("clien_dataalteracao").alias("data_alteracao")
        )
    )

    df_new = (
        df_select1.withColumn("tipo_logradouro", F.lit(None).cast("string"))
                 .withColumn("bairro", F.lit(None).cast("string"))
    ).select("cpf","cep", "complemento", "logradouro", "cidade", "data_alteracao","tipo_logradouro","bairro","uf") 
    

    return df_new

def tabela_atualizada(df1,df2,df3):

    df_union = df1.union(df2).union(df3)

    window_spec = Window.partitionBy(F.col("cpf")).orderBy(F.col("data_alteracao").desc())
    df_final = df_union.withColumn("row_number", F.row_number().over(window_spec))\
             .filter(F.col("row_number") == 1)

    df_final.write.format("parquet")\
                  .mode("overwrite")\
                  .save(r".\docs\silver")


#    df_union.filter((F.col("cidade") == "Rebeccaton") | (F.col("cidade") == "Tinamouth") | (F.col("cidade") == "Leonardville") ).show(20,False)

    return True



    
