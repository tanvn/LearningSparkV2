package chapter2


import java.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Usage: GroupByTest [numMappers] [numKVPairs] [valSize] [numReducers]
 */
object GroupByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GroupBy Test").setMaster("local[5]")

    val numMappers = 100
    val numKVPairs = 10000
    val valSize = 1000
    val numReducers = 36

    val sc = new SparkContext(sparkConf)


    // https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.mapPartitions.html
    val pairs1: RDD[(Int, Array[Byte])] = sc.parallelize(0 until numMappers, numMappers).flatMap { _ =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)

      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(100), byteArr)
      }

      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    val totalElements = pairs1.count
    println(totalElements)

    val groupByKey: RDD[(Int, Iterable[Array[Byte]])] = pairs1.groupByKey(numReducers)

//    groupByKey.foreach{ case (k, v) =>
//      println(k + " : " + v.size)
//    }

    println(groupByKey.toDebugString)


    println(groupByKey.count)

    Thread.sleep(300000)

    sc.stop()
  }
}
