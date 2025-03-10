from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import save_to_bronze as save_bronze
import silver_read_to_file 
import save_bronze_to_silver as save_to_silver

def main():
    spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()

    #PRIMEIRO TEREI QUE CARREGAR OS ARQUIVOS CSV E SALVA-LOS NA BRONZE

    #ATUALIZAÇÃO CADASTRAL
    path_doc_atucad_read = r".\docs\atucad"
    path_bronze_atucad_write = r".\docs\bronze\atucad_bronze"

    processing_atucad = save_bronze.atucad_to_load(spark)
    processing_atucad.processing_atucad_bronze(path_doc_atucad_read,path_bronze_atucad_write)

    #CUSTOMEREXP
    path_doc_customerexp_read = r".\docs\customerexp"
    path_doc_customerexp_write = r".\docs\bronze\customerexp_bronze"

    processing_custmerexp = save_bronze.customerexp_to_load(spark)
    processing_custmerexp.processing_customerexp_bronze(path_doc_customerexp_read,path_doc_customerexp_write)

    #PORTAL CLIENTE
    path_bronze_portal_cliente_read = r".\docs\portalcliente"
    path_bronze_portal_cliente_write = r".\docs\bronze\portal_cliente_bronze"
    
    processing_portal_cliente = save_bronze.portal_cliente_to_load(spark)
    processing_portal_cliente.processing_portalcliente_bronze(path_bronze_portal_cliente_read,path_bronze_portal_cliente_write)


    #SEGUNDO TEREI QUE CARREGAR OS DADOS DA BRONZE REALIZAR TRANSFORMAÇÕES E SALVAR NA SILVER

    #ATUALIZAÇÃO CADASTRAL
    transformation_atucad = save_to_silver.transformation_atucad(spark)
    df_atucad = transformation_atucad.processing_atucad(path_bronze_atucad_write)
    df_atucad.show()

    #CUSTOMEREXP
    transformation_customerexp = save_to_silver.transformation_customerexp(spark)
    df_customerexp = transformation_customerexp.processing_customerexp(path_doc_customerexp_write)
    df_customerexp.show()
    
    #PORTAL CLIENTE
    transformation_portalcliente = save_to_silver.transformation_portalcliente(spark)
    df_portal_cliente = transformation_portalcliente.processing_portalcliente(path_bronze_portal_cliente_write)
    df_portal_cliente.show()

    save_to_silver.tabela_atualizada(df1=df_atucad,df2=df_customerexp,df3=df_portal_cliente)



    #LEANDO A SILVER 
    path_doc = r".\docs\silver"

    processo = silver_read_to_file.readSilver(spark)

    processo.silver_read(path_doc)
if __name__ == "__main__":
    main()
