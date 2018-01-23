package ddcw.tutorial.spark

import org.apache.spark.SparkContext

object Step3 {
  def main(args: Array[String]) : Unit = {
    ExecuteSpark.execute(answerQuestion)
  }

  def answerQuestion(sc: SparkContext) : Unit = {
    val flightDataWithHeader = sc.textFile("data/flights.csv")
    val flightDataAsString = ExecuteSpark.dropHeader(flightDataWithHeader)

    val flightData = flightDataAsString
      .map(flight => flight.split(",").toList)

    flightData.take(5).foreach(println)
  }
}
