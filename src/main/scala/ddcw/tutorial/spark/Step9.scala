package ddcw.tutorial.spark

import org.apache.spark.SparkContext

object Step9 {
  def main(args: Array[String]) : Unit = {
    ExecuteSpark.execute(answerQuestion)
  }

  def answerQuestion(sc: SparkContext) : Unit = {
    val airportDataWithHeader = sc.textFile("data/airports.csv")
    val airportDataAsString = ExecuteSpark.dropHeader(airportDataWithHeader)
    val airportData = airportDataAsString
      .map(airport => airport.split(",").toList)
      .map(airport => (airport.head, airport(3))).cache()

    val flightDataWithHeader = sc.textFile("data/flights.csv")
    val flightDataAsString = ExecuteSpark.dropHeader(flightDataWithHeader)
    val flightData = flightDataAsString
      .map(flight => flight.split(",").toList)
      .map(flight => ((flight(7), flight(8)), 1))
      .reduceByKey(_ + _).cache()

    val answerToQuestion = flightData
      .map(flight => (flight._1._1, (flight._1._2, flight._2)))
      .join(airportData)
      .map(flight => (flight._2._1._1, (flight._2._2, flight._2._1._2)))
      .join(airportData)
      .map(flight => ((flight._2._1._1, flight._2._2), flight._2._1._2))
      .filter(flight => !flight._1._1.equals(flight._1._2))
      .reduceByKey(_ + _)
      .map(item => item.swap)
      .sortByKey(ascending = false)

    answerToQuestion.take(5).foreach(println)
  }
}
