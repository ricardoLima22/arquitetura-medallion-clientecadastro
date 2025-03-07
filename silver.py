from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").appName("bronzemain").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()


def main():

    path_doc = r".\docs\silver"

    df_silver = spark.read\
                   .format("parquet")\
                   .load(path_doc)
    
    df_silver.filter((F.col("cidade") == "Rebeccaton") | (F.col("cidade") == "Tinamouth") | (F.col("cidade") == "Leonardville")).show(20, False)

    df_silver.show(20, False)
if __name__ == "__main__":
    main()

