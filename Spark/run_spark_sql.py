import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#The main function taking csv file path & SQL file path as arguments
def main(csv_path: str, sql_file: str):
    spark = (SparkSession.builder
             .appName("SparkSQL-CSV-Analytics")
             .getOrCreate())

    # Read CSV(s)
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(csv_path))

    # Register a temp view for Spark SQL
    df.createOrReplaceTempView("maintenance_events")

    # Load SQL from file and run it
    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()

    result = spark.sql(query)
    result.show(truncate=False)
    
    #Stopping the SparkSession
    spark.stop()

#Validating 3 arguments are passed otherwise output the prompt passed in the SystemExit method
if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: spark-submit run_spark_sql.py '<csv_path>' '<sql_file>'")
    main(sys.argv[1], sys.argv[2])
