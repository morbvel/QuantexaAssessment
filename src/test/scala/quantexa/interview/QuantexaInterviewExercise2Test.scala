package quantexa.interview

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions.col

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewExercise2Test extends SparkTesting {
  import spark.implicits._

  "getFlightsFromPassenger" should "return the passenger with most flights" in {
    val flights = List(
      ("1"), ("1"), ("1"), ("1"), ("2"), ("2"), ("3"), ("3"), ("3"), ("3"), ("3"), ("3"), ("1"), ("4"), ("4"), ("4")
    ).toDF("passengerId")

    val results = QuantexaInterviewExercise2.getFlightsFromPassenger(flights)
      .withColumn("id", col("passengerId"))

    assert(5 == getValueFromId[Int](results, 1, "Number of flights"))
    assert(2 == getValueFromId[Int](results, 2, "Number of flights"))
    assert(6 == getValueFromId[Int](results, 3, "Number of flights"))
    assert(3 == getValueFromId[Int](results, 4, "Number of flights"))
  }

  "getPassengerswithMoreFlights" should "return the most frequent passangers" in {
    val passangers = List(
      ("1", "Passanger1", "LastName1"),
      ("2", "Passanger2", "LastName2"),
      ("3", "Passanger3", "LastName3"),
      ("4", "Passanger4", "LastName4")
    ).toDF("passengerId", "firstName", "lastName")

    val flights = List(
      ("1"), ("1"), ("1"), ("1"), ("2"), ("2"), ("3"), ("3"), ("3"), ("3"), ("3"), ("3"), ("1"), ("4"), ("4"), ("4")
    ).toDF("passengerId")

    val results = QuantexaInterviewExercise2.getPassengersWithMoreFlights(passangers, flights)

    val idDf = List(
      (1, "1"), (2, "2"), (3, "3"), (4, "4")
    ).toDF("id", "passengerId")

    val finalResults = results.join(
      idDf, Seq("passengerId"), "left_outer"
    )

    assert( 6 == getValueFromId[Int](finalResults, 3,"Number of flights") )
    assert( 5 == getValueFromId[Int](finalResults, 1,"Number of flights") )
    assert( 3 == getValueFromId[Int](finalResults, 4,"Number of flights") )
    assert( 2 == getValueFromId[Int](finalResults, 2, "Number of flights") )
  }
}
