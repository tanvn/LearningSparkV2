package chapter2

import org.apache.spark.sql.SparkSession

import java.io.File


object SelectOrcTable {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder
      .appName("SelectOrcTable")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/Users/jp28431/github/tanvn/LearningSparkV2/chapter2/scala/src/main/resources/log4j2.properties")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=/Users/jp28431/github/tanvn/LearningSparkV2/chapter2/scala/src/main/resources/log4j2.properties")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

//    val desc_table = spark.sql("SHOW CREATE TABLE spark_db.users")
//    desc_table.show(false)

    val result = spark.sql("SELECT * FROM spark_db.users WHERE age > 30")
    result.explain(true)
    result.show(false)

//    val partitionNum = result.rdd.getNumPartitions
//    println(s"Number of partitions: $partitionNum")

    // Stop the Spark session
    Thread.sleep(100000) // Sleep for 100 seconds
    spark.stop()
  }

}