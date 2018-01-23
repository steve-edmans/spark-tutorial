package ddcw.tutorial.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.StdIn

object ExecuteSpark {
  def execute(func: SparkContext => Unit, withPause : Boolean = true) : Unit = {
    val sc = new SparkContext("local[12]", "Simple App")
    func(sc)
    println()
    if (withPause) {
      println("Press ENTER to finish")
      StdIn.readLine()
    }
    sc.stop()
  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex {
      (idx, iter) =>
        if (idx == 0)
          iter.drop(1)
        else
          iter
    }
  }
}
