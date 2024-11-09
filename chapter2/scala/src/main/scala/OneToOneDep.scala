import org.apache.spark.{SparkConf, SparkContext}

object OneToOneDep {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OneToOneDependencyExample").setMaster("local")
    val sc = new SparkContext(conf)

    // Creating a parent RDD
    val data = Array(1, 2, 3, 4, 5)
    val parentRDD = sc.parallelize(data, 2) // 2 partitions

    // Applying a transformation that maintains OneToOneDependency
    val childRDD = parentRDD.map(x => x * 2)

    // Action to collect and print the results
    val result = childRDD.collect()

    println(result.mkString(", "))

    sc.stop()
  }

}
