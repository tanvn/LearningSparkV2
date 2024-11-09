package chapter2

import org.apache.spark.sql.SparkSession

object InsertOrcTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InsertOrcTable")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/Users/jp28431/github/tanvn/LearningSparkV2/chapter2/scala/src/main/resources/log4j2.properties")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=/Users/jp28431/github/tanvn/LearningSparkV2/chapter2/scala/src/main/resources/log4j2.properties")
      .master("local[4]")
      .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS spark_db")
    spark.sql("USE spark_db")
    spark.sql("CREATE TABLE IF NOT EXISTS spark_db.users (name STRING, age INT) USING ORC")
    spark.sql(
      """INSERT INTO spark_db.users VALUES
        |('John', 30), ('Alice', 25), ('Bob', 28), ('Cathy', 23), ('David', 27), ('Eva', 24), ('Frank', 26), ('Grace', 22),
        |('Henry', 29), ('Ivy', 21), ('Jack', 31), ('Kelly', 20), ('Vicky', 32), ('Wendy', 33), ('Xavier', 34), ('Yvonne', 35),
        |('Zack', 36), ('Quinn', 37)""".stripMargin)

    //    spark.sql("CREATE TABLE IF NOT EXISTS mydb.des_tbl (name STRING, age INT) USING PARQUET")
//    spark.sql("INSERT OVERWRITE mydb.des_tbl SELECT * FROM mydb.src_tbl WHERE age >= 20")

    val desc_table = spark.sql("SHOW CREATE TABLE spark_db.users")
    desc_table.show(false)

    val result = spark.sql("SELECT * FROM spark_db.users WHERE age > 30")
    val partitionNum = result.rdd.getNumPartitions
    println(s"Number of partitions: $partitionNum")
    result.show(false)

    // Stop the Spark session
    Thread.sleep(100000) // Sleep for 100 seconds
    spark.stop()
  }

}