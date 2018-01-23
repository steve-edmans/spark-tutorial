package ddcw.tutorial.spark

import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object Step8 {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().appName("Spark SQL Example").master("local[12]").getOrCreate()

    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/flights.csv")
      .createOrReplaceTempView("flights")

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/airports.csv")
      .createOrReplaceTempView("airports")

    val sql =
      """ SELECT
        |     o.STATE OriginState,
        |     d.STATE DestinationState,
        |     COUNT(*) NumberOfFlights
        | FROM flights f
        | JOIN airports o
        | ON f.ORIGIN_AIRPORT = o.IATA_CODE
        | JOIN airports d
        | ON f.DESTINATION_AIRPORT = d.IATA_CODE
        | WHERE o.STATE != d.STATE
        | GROUP BY o.STATE, d.STATE
        | ORDER BY 3 DESC
      """.stripMargin

    val sqlDF = spark.sql(sql)
    sqlDF.show(5)

    println("Press ENTER to finish")
    StdIn.readLine()

    spark.stop()
  }
}
