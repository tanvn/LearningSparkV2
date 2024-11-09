package chapter2

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object InsertCommand{
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("CreateTableExample")
      .master("local[4]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      Row("John", 30),
      Row("Alice", 25),
      Row("Bob", 28)
    )

    // Define the schema for the DataFrame
    val schema = StructType(List(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)
    ))

    // Create a DataFrame with the sample data and schema
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Show the DataFrame
    df.show()

    // Save the DataFrame as a table (temporary in this case)
    val tableName = "mytable"
    df.createOrReplaceTempView(tableName)

    // Execute a SQL query to select from the created table
    val result = spark.sql(s"SELECT * FROM $tableName")
    result.show()

    // Stop the Spark session
    spark.stop()
  }
}
