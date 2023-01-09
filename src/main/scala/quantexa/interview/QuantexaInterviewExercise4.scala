package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, max, min}

object QuantexaInterviewExercise4 {

  def getPassengersInFlightsTogether(flightsDf: DataFrame, minimumOfFlightsTogether: Int): DataFrame =

    flightsDf
      .transform(getRenamedPassengers)
      .transform(getInnerPassengers(flightsDf))
      .transform(getNumberOfFlightsTogether)
      .filter(col("Number of flights together") > minimumOfFlightsTogether)
      .orderBy(col("passengerId").asc)

  def getRenamedPassengers(inputDf: DataFrame): DataFrame =

    inputDf
      .withColumnRenamed("passengerId", "passengerId2")
      .select("passengerId2", "flightId")

  def getInnerPassengers(inputDf: DataFrame)(originalDf: DataFrame): DataFrame =

    originalDf
      .join( inputDf, Seq("flightId"), "inner" )
      .select("passengerId", "passengerId2", "flightId", "date")
      .filter( col("passengerId").notEqual(col("passengerId2")) )

  def getNumberOfFlightsTogether(inputDf: DataFrame): DataFrame =

    inputDf
      .groupBy("passengerId", "passengerId2")
      .agg(
        count("flightId") as "Number of flights together",
        min("date") as "From",
        max("date") as "To"
      )
}
