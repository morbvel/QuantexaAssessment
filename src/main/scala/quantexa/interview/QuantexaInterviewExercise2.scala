package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{count, col}

object QuantexaInterviewExercise2 {

  def getPassengersWithMoreFlights(passengers: DataFrame, flights: DataFrame): DataFrame =

    passengers.join(
      getFlightsFromPassenger(flights),
      Seq("passengerId"),
      "left_outer"
    ).orderBy(col("Number of flights").desc).limit(100)

  def getFlightsFromPassenger(inputDf: DataFrame): DataFrame =

    inputDf.groupBy("passengerId")
      .agg(count("passengerId").cast(IntegerType) as "Number of flights")
}
