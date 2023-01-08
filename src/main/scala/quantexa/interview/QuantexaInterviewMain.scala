package quantexa.interview

import org.apache.spark.sql.{DataFrame, SparkSession}

object QuantexaInterviewMain extends App with Constants{

  implicit val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

  val rawPassangersTable: DataFrame = QuantexaInterviewSources.getSourceData(PASSANGER_DATA_PATH, PASSANGERS_TABLE)
  val rawFlightsTable: DataFrame = QuantexaInterviewSources.getSourceData(FLIGHT_DATA_PATH, FLIGHTS_TABLE)

  // Exersise 1
  val numberOfFlightsPerMonth = QuantexaInterviewExercise1.getFlightsPerMonth(rawFlightsTable)
  numberOfFlightsPerMonth.show(false)

  // Exercise 2
  val passangersWithMoreFlights = QuantexaInterviewExercise2.getPassengersWithMoreFlights(rawPassangersTable, rawFlightsTable)
  passangersWithMoreFlights.show(false)

  // Exercise 3
  val longestRunForPassangers = QuantexaInterviewExercise3.getLongestRun(rawFlightsTable)
  longestRunForPassangers.show(false)

  // Exercise 4
  val passengersTogether = QuantexaInterviewExercise4.getPassengersInFlightsTogether(rawFlightsTable, 3)
  passengersTogether.show(false)

  // Extra Exercise
  val passengersTogetherByDate = FlightsExcercisesExtraExercise.
    getPassengersInFlightsTogetherByDate(rawFlightsTable, 3, "2020-01-01", "2020-012-31")
  passengersTogetherByDate.show(false)

}
