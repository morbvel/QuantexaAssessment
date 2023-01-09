package quantexa.interview

import org.apache.spark.sql.{DataFrame, SparkSession}

object QuantexaInterviewMain extends App with Constants{

  def executeExercise1(rawFlightsTable: DataFrame): Unit = {
    QuantexaInterviewExercise1.getFlightsPerMonth(rawFlightsTable).show(false)
    println("------------Exercise 1---------------")
  }

  def executeExercise2(rawPassangersTable: DataFrame, rawFlightsTable: DataFrame): Unit = {
    QuantexaInterviewExercise2.getPassengersWithMoreFlights(rawPassangersTable, rawFlightsTable).show(false)
    println("------------Exercise 2---------------")
  }

  def executeExercise3(rawFlightsTable: DataFrame): Unit = {
    QuantexaInterviewExercise3.getLongestRun(rawFlightsTable).show(false)
    println("------------Exercise 3---------------")
  }

  def executeExercise4(rawFlightsTable: DataFrame, minFlights: Int = 3): Unit = {
    QuantexaInterviewExercise4.getPassengersInFlightsTogether(rawFlightsTable, minFlights).show(false)
    println("------------Exercise 4---------------")
  }

  def executeExtraExercise(
    rawFlightsTable: DataFrame,
    minflights: Int = 3,
    startDate: String = "2017-10-01",
    endDate: String = "2017-12-31"
  ): Unit = {
    QuantexaInterviewExtraExercise
      .getPassengersInFlightsTogetherByDate(rawFlightsTable, minflights, startDate, endDate)
      .show(false)
    println("---------------Extra Exercise------------------")
  }

  def executeExercises(): Unit = {

    implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    val rawPassangersTable: DataFrame = QuantexaInterviewSources.getSourceData(PASSANGER_DATA_PATH, PASSANGERS_TABLE)
    val rawFlightsTable: DataFrame = QuantexaInterviewSources.getSourceData(FLIGHT_DATA_PATH, FLIGHTS_TABLE)

    executeExercise1(rawFlightsTable)
    executeExercise2(rawPassangersTable, rawFlightsTable)
    executeExercise3(rawFlightsTable)
    executeExercise4(rawFlightsTable)
    executeExtraExercise(rawFlightsTable)
  }


}
