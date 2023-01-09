package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexInterviewExtraExerciseTest extends SparkTesting {
  import spark.implicits._

  "getPassangersInFlightsTogetherByDate" should "return passengers who have flight more than N times together in a date range" in {
    val flights = List(
      ("1", "1", "2020-01-04"), ("1", "2", "2020-01-06"), ("1", "3", "2020-01-08"),
      ("1", "4", "2020-01-10"), ("1", "5", "2020-01-12"), ("1", "6", "2020-01-14"),
      ("1", "7", "2020-01-16"),
      ("2", "1", "2020-01-04"), ("2", "2", "2020-01-06"), ("2", "3", "2020-01-08"),
      ("2", "4", "2020-01-10"), ("2", "5", "2020-01-12"),
      ("3", "4", "2020-01-10"), ("3", "5", "2020-01-12"), ("3", "6", "2020-01-14"),
      ("3", "7", "2020-01-16")
    ).toDF("passengerId", "flightId", "date")

    val results = QuantexaInterviewExtraExercise
      .getPassengersInFlightsTogetherByDate(flights, 3, "2020-01-06", "2020-01-16")

    val groupedByPassenger = results
      .groupBy("passengerId").count()
      .withColumn("id", col("passengerId"))

    val numberOfFlights = "count"
    val startDate = "From"
    val endDate = "To"

    assert( 2 == getValueFromId[Long](groupedByPassenger, 1, numberOfFlights) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 2, numberOfFlights) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 3, numberOfFlights) )

    assert( "2020-01-10" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "3"), 1, startDate)
    )
    assert( "2020-01-16" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "3"), 1, endDate)
    )
    assert( "2020-01-06" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "2"), 1, startDate)
    )
    assert( "2020-01-12" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "2"), 1, endDate)
    )
    assert( "2020-01-06" == getValueFromId[String]
      (filterPassangersFlights(results, "2", "1"), 1, startDate)
    )
    assert( "2020-01-12" == getValueFromId[String]
      (filterPassangersFlights(results, "2", "1"), 1, endDate)
    )
    assert( "2020-01-10" == getValueFromId[String]
      (filterPassangersFlights(results, "3", "1"), 1, startDate)
    )
    assert( "2020-01-16" == getValueFromId[String]
      (filterPassangersFlights(results, "3", "1"), 1, endDate)
    )

  }

  private def filterPassangersFlights(inputDf: DataFrame, passenger1: String, passenger2: String): DataFrame =
    inputDf.filter(col("passengerId").equalTo(passenger1) && col("passengerId2").equalTo(passenger2))
    .withColumn("id", lit(1))

}
