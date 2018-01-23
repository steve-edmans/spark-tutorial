package ddcw.tutorial.spark

import org.apache.spark.SparkContext

object Step1 {
  def main(args: Array[String]) : Unit = {
    ExecuteSpark.execute(answerQuestion)
  }

  def answerQuestion(sc: SparkContext) : Unit = {
    val flightData = sc.textFile("data/flights.csv")
    flightData.take(5).foreach(println)
  }
}
