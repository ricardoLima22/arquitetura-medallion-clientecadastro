from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import silver_read_to_file as atual_silver



def main():
    spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()

    path_doc = r".\docs\silver"
    
    processor = atual_silver.readSilver(spark=spark)

    df_teste = processor.silver_read(path_doc)


    teste = atual_silver.silver_transformation(spark)
    teste.teste(df_teste)
if __name__ == "__main__":
    main()

