package ddcw.tutorial.spark

import org.apache.spark.SparkContext

object Step2 {
  def main(args: Array[String]) : Unit = {
    ExecuteSpark.execute(answerQuestion)
  }

  def answerQuestion(sc: SparkContext) : Unit = {
    val flightDataWithHeader = sc.textFile("data/flights.csv")
    val flightData = flightDataWithHeader
      .mapPartitionsWithIndex {
        (idx, iter) =>
          if (idx == 0)
            iter.drop(1)
          else
            iter
      }
    flightData.take(5).foreach(println)
  }
}
