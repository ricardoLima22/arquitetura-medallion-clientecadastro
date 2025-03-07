from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import save_to_bronze as save_bronze
import save_bronze_to_silver as save_to_silver

def main():
    spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()

    #PRIMEIRO TEREI QUE CARREGAR OS ARQUIVOS CSV E SALVA-LOS NA BRONZE

    #ATUALIZAÇÃO CADASTRAL
    path_doc_atucad_read = r".\docs\atucad"
    path_bronze_atucad_write = r".\docs\bronze\atucad_bronze"

    save_bronze.processing_atucad_bronze(path_read=path_doc_atucad_read,path_write=path_bronze_atucad_write,spark=spark)

    #CUSTOMEREXP
    path_doc_customerexp_read = r".\docs\customerexp"
    path_doc_customerexp_write = r".\docs\bronze\customerexp_bronze"

    save_bronze.processing_customerexp_bronze(path_read=path_doc_customerexp_read,path_write=path_doc_customerexp_write,spark=spark)

    #PORTAL CLIENTE
    path_bronze_portal_cliente_read = r".\docs\portalcliente"
    path_bronze_portal_cliente_write = r".\docs\bronze\portal_cliente_bronze"
    

    save_bronze.processing_portalcliente_bronze(path_read=path_bronze_portal_cliente_read,path_write=path_bronze_portal_cliente_write,spark=spark)



    #SEGUNDO TEREI QUE CARREGAR OS DADOS DA BRONZE REALIZAR TRANSFORMAÇÕES E SALVAR NA SILVER

    #ATUALIZAÇÃO CADASTRAL
    df_atucad = save_to_silver.processing_atucad(path=path_bronze_atucad_write,spark=spark)
    df_atucad.show()
    
    #CUSTOMEREXP
    df_customerexp = save_to_silver.processing_customerexp(path=path_doc_customerexp_write,spark=spark)
    df_customerexp.show()
    
    #PORTAL CLIENTE
    df_portal_cliente = save_to_silver.processing_portalcliente(path=path_bronze_portal_cliente_write,spark=spark)
    df_portal_cliente.show()

    save_to_silver.tabela_atualizada(df1=df_atucad,df2=df_customerexp,df3=df_portal_cliente)



if __name__ == "__main__":
    main()
