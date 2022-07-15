
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
            .appName("twitter_transformation")\
                .getOrCreate()
    
    df = spark.read.json(
        "/home/ritaalamino/workspace/observatorio/curso_data_lake/AIRFLOW/datalake/twitter_nlp/extract_date=2022-07-06"
    )
    df.printSchema()
    df.show()
