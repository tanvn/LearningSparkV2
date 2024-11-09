import org.apache.spark.rdd.RDD

object CogroupedRDDTest {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}

    val conf = new SparkConf().setAppName("CoGroupedRDDExample").setMaster("local")
    val sc = new SparkContext(conf)

    // Create two RDDs with key-value pairs
    val rdd1: RDD[(Int, String)] = sc.parallelize(Seq((1, "A"), (2, "B"), (3, "C")))
    val rdd2: RDD[(Int, String)] = sc.parallelize(Seq((1, "X"), (2, "Y"), (3, "Z")))

    // Apply cogroup transformation
    // Both rdd1 and rdd2 have their partitioner equal to None (default value of RDD), therefore getDependencies will return a list of
    // 2 ShuffleDependency instances in the intermediate CoGroupedRDD created inside rdd1.cogroup(rdd2)

    val coGroupedRDD: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)

    // Collect and print the result
    coGroupedRDD.collect().foreach {
      case (key, (values1, values2)) =>
        println(s"Key: $key, Values from RDD1: ${values1.mkString(", ")}, Values from RDD2: ${values2.mkString(", ")}")
    }
    sc.stop()
  }
}
