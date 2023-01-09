package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lit, lower, row_number, when, max}

object QuantexaInterviewExercise3 {

  def getLongestRun(flights: DataFrame): DataFrame =

    flights
      .transform(getStartsArriveUK)
      .transform(getNumberOfFlightsPerPassenger)
      .transform(getFlightsFromOrToUk)
      .transform(getPreviousJourneyStart)
      .transform(getPreviousJourneyStage)
      .transform(getTotalJourneyStages)
      .groupBy("passengerId").agg(max("totalFlightsInJourney") as "totalFlightsInJourney")

  def getStartsArriveUK(inputDf: DataFrame): DataFrame = {

    inputDf.withColumn("startsArrivesUk",
      when(
        lower(col("from")).equalTo("uk"),
        lit(1)
      ).when(
        lower(col("to")).equalTo("uk"),
        lit(2)
      ).otherwise(0)
    ).orderBy(col("date").desc)
  }

  def getNumberOfFlightsPerPassenger(inputDf: DataFrame): DataFrame =

    inputDf.withColumn(
      "row_number",
      row_number.over(Window.partitionBy("passengerId").orderBy("date"))
    )

  def getFlightsFromOrToUk(inputDf: DataFrame): DataFrame =

    inputDf.filter(col("startsArrivesUk").isin(1,2))

  def getPreviousJourneyStart(inputDf: DataFrame): DataFrame =

    inputDf.withColumn(
      "totalFlightsInJourney",
      lag(col("startsArrivesUk"), 1, null).over(
        Window.partitionBy("passengerId").orderBy("date"))
    )

  def getTotalJourneyStages(inputDf: DataFrame): DataFrame =

    inputDf
      .filter(col("totalFlightsInJourney").notEqual(0))
      .withColumn("totalFlightsInJourney", col("row_number") - col("totalFlightsInJourney"))

  def getPreviousJourneyStage(inputDf: DataFrame): DataFrame =

    inputDf.withColumn(
      "totalFlightsInJourney",
      when(
        col("totalFlightsInJourney").equalTo(1),
        lag(col("row_number"), 1, null).over(
          Window.partitionBy("passengerId").orderBy("date")
        )
      ).otherwise(0)
    )
}
