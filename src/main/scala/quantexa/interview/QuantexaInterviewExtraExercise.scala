package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col


object QuantexaInterviewExtraExercise {

  def getPassengersInFlightsTogetherByDate(
    flights: DataFrame,
    minimumOfFlightsTogether: Int,
    startDate: String,
    endDate: String): DataFrame = {

    val flightsInDates = flights
      .filter(
        col("date").geq(startDate) && col("date").leq(endDate)
      )

    QuantexaInterviewExercise4.getPassengersInFlightsTogether(flightsInDates, minimumOfFlightsTogether)
  }
}