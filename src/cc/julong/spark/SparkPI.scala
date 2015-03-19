package cc.julong.spark

import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPI {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("spark://hadoop01:7077")
    conf.setAppName("sparkPi")
    val spark = new SparkContext(conf)
    val slices = 2//if (args.length > 0) args(0).toInt else 2
    val n = 100 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}