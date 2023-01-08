package quantexa.interview

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, row_number}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewExercise3Test extends SparkTesting{
  import spark.implicits._

  "getStartsArriveUK" should "set a flag whether a flight comes from or to the UK" in {
    val inputDf = List(
      ("1", "uk", "dummy", "2023-01-01"),
      ("2", "dummy", "dummy", "2023-01-02"),
      ("3", "dummy", "dummy", "2023-01-03"),
      ("4", "dummy", "uk", "2023-01-04")
    ).toDF("id", "from", "to", "date")

    val results = QuantexaInterviewExercise3.getStartsArriveUK(inputDf)

    assert(1 == getValueFromId[Int](results, 1, "startsArrivesUk"))
    assert(0 == getValueFromId[Int](results, 2, "startsArrivesUk"))
    assert(0 == getValueFromId[Int](results, 3, "startsArrivesUk"))
    assert(2 == getValueFromId[Int](results, 4, "startsArrivesUk"))
  }

  "getNumberOfFlightsPerPassenger" should "return the ammount of flights per passenger" in {
    val inputDf = List(
      ("1", "2023-01-01"),
      ("1", "2023-01-02"),
      ("1", "2023-01-03"),
      ("1", "2023-01-04"),
      ("2", "2023-01-01"),
      ("2", "2023-01-02"),
      ("3", "2023-01-01")
    ).toDF("passengerId", "date")

    val results = QuantexaInterviewExercise3.getNumberOfFlightsPerPassenger(inputDf)
      .groupBy("passengerId").agg(max("row_number") as "count")
      .withColumn("id", col("passengerId"))

    assert(4 == getValueFromId[Int](results, 1, "count"))
    assert(2 == getValueFromId[Int](results, 2, "count"))
    assert(1 == getValueFromId[Int](results, 3, "count"))
  }

  "getFlightsFromOrToUk" should "return only those flights which have started or ended in the UK" in {
    val inputDf = List(
      ("1", 0),
      ("2", 1),
      ("3", 1),
      ("4", 2),
    ).toDF("id", "startsArrivesUk")

    val results = QuantexaInterviewExercise3.getFlightsFromOrToUk(inputDf)

    assert(1 == getValueFromId[Int](results, 2, "startsArrivesUk"))
    assert(1 == getValueFromId[Int](results, 3, "startsArrivesUk"))
    assert(2 == getValueFromId[Int](results, 4, "startsArrivesUk"))
  }

  "getPreviousJourneyStart" should "return the previous starting point" in {
    val inputDf = List(
      (1, "1", "2023-01-01"),
      (0, "1", "2023-01-02"),
      (2, "1", "2023-01-03")
    ).toDF("startsArrivesUk", "passengerId", "date")

    val results = QuantexaInterviewExercise3.getPreviousJourneyStart(inputDf)
        .withColumn("id", row_number.over(Window.partitionBy("passengerId").orderBy("date")))

    assert(null == getValueFromId[String](results, 1, "totalFlightsInJourney"))
    assert(1 == getValueFromId[Int](results, 2, "totalFlightsInJourney"))
    assert(0 == getValueFromId[Int](results, 3, "totalFlightsInJourney"))
  }

  "getPreviousJourneyStage" should "return the previous step stage number of the journey" in {
    val inputDf = List(
      (0, 5, "1", "2023-01-01"),
      (1, 6, "1", "2023-01-02"),
      (2, 7, "1", "2023-01-03")
    ).toDF("totalFlightsInJourney", "row_number", "passengerId", "date")

    val results = QuantexaInterviewExercise3.getPreviousJourneyStage(inputDf)
        .withColumn("id", col("row_number"))

    assert(0 == getValueFromId[Int](results, 5, "totalFlightsInJourney"))
    assert(5 == getValueFromId[Int](results, 6, "totalFlightsInJourney"))
    assert(0 == getValueFromId[Int](results, 7, "totalFlightsInJourney"))
  }

  "getTotalJourneyStages" should "return the total ammount of flights since last time the passenger flew form the UK" in {
    val inputDf = List(
      ("1", "3", "5"),
      ("2", "1", "4"),
      ("3", "2", "8")
    ).toDF("id", "totalFlightsInJourney", "row_number")

    val results = QuantexaInterviewExercise3.getTotalJourneyStages(inputDf)

    assert(2.0 == getValueFromId[Double](results, 1, "totalFlightsInJourney"))
    assert(3.0 == getValueFromId[Double](results, 2, "totalFlightsInJourney"))
    assert(6.0 == getValueFromId[Double](results, 3, "totalFlightsInJourney"))
  }

  "getLongestRun" should "return the longest run from and to the UK for each passenger" in{
    val inputDf = List(
      ("1", "1", "UK", "Spain", "2020-01-01"),
      ("1", "2", "Spain", "Italy", "2020-01-02"),
      ("1", "3", "Italy", "China", "2020-01-03"),
      ("1", "4", "China", "Australia", "2020-01-04"),
      ("1", "5", "Australia", "Ireland", "2020-01-05"),
      ("1", "6", "Ireland", "Portugal", "2020-01-06"),
      ("1", "7", "Portugal", "UK", "2020-01-07"),
      ("1", "8", "UK", "USA", "2020-01-08"),
      ("1", "9", "USA", "Canada", "2020-01-09"),
      ("1", "10", "Canada", "UK", "2020-01-10"),
      ("1", "11", "UK", "Denmark", "2020-01-11"),
      ("1", "12", "Denmark", "Russia", "2020-01-12"),
      ("1", "13", "Russia", "Greece", "2020-01-13"),
      ("1", "14", "Greece", "Japan", "2020-01-14"),
      ("1", "14", "Japan", "UK", "2020-01-15"),
      ("2", "15", "UK", "Ireland", "2020-01-16"),
      ("2", "16", "Ireland", "Andorra", "2020-01-17"),
      ("2", "17", "Andorra", "UK", "2020-01-18")
    ).toDF("passengerId", "flightId", "from", "to", "date")

    val results = QuantexaInterviewExercise3.getLongestRun(inputDf)
        .withColumn("id", col("passengerId"))

    assert(6 == getValueFromId[Int](results, 1, "totalFlightsInJourney"))
    assert(2 == getValueFromId[Int](results, 2, "totalFlightsInJourney"))
  }
}
