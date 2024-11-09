package chapter2

import org.apache.spark.sql.SparkSession

object InsertIntoTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InsertIntoTableExample")
      .master("local[2]")
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS mydb")
    spark.sql("USE mydb")
    spark.sql("CREATE TABLE IF NOT EXISTS mydb.src_tbl (name STRING, age INT) USING PARQUET")
    spark.sql("INSERT INTO mydb.src_tbl VALUES ('John', 30), ('Alice', 25), ('Bob', 28), ('Cathy', 23), ('David', 27), ('Eva', 24)")
    spark.sql("CREATE TABLE IF NOT EXISTS mydb.des_tbl (name STRING, age INT) USING PARQUET")
    spark.sql("INSERT OVERWRITE mydb.des_tbl SELECT * FROM mydb.src_tbl WHERE age >= 20")

    val result = spark.sql("SELECT * FROM mydb.des_tbl")
    val partitionNum = result.rdd.getNumPartitions
    println(s"Number of partitions: $partitionNum")
    result.show()
    // Stop the Spark session
    spark.stop()
  }

}
