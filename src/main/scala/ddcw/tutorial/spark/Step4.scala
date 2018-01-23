package ddcw.tutorial.spark

import org.apache.spark.SparkContext

object Step4 {
  def main(args: Array[String]) : Unit = {
    ExecuteSpark.execute(answerQuestion)
  }

  def answerQuestion(sc: SparkContext) : Unit = {
    val flightDataWithHeader = sc.textFile("data/flights.csv")
    val flightDataAsString = ExecuteSpark.dropHeader(flightDataWithHeader)
    val flightData = flightDataAsString
      .map(flight => flight.split(",").toList)
      .map(flight => (flight(7), (flight(8), flight(4), flight(11))))

    val airportDataWithHeader = sc.textFile("data/airports.csv")
    val airportDataAsString = ExecuteSpark.dropHeader(airportDataWithHeader)
    val airportData = airportDataAsString
      .map(airport => airport.split(",").toList)
      .map(airport => (airport.head, airport(3)))

    val flightWithOriginState = flightData
      .join(airportData)

    flightWithOriginState.take(5).foreach(println)
  }
}
