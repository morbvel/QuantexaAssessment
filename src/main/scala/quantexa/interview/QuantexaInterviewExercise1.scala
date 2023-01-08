package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, to_date}

object QuantexaInterviewExercise1 {

  def getFlightsPerMonth(inputDf: DataFrame): DataFrame =

    inputDf
      .transform(getMonthValueColumn)
      .groupBy("month").count

  def getMonthValueColumn(inputDf: DataFrame): DataFrame =

    inputDf.withColumn(
      "month",
      date_format(to_date(col("date"), "yyyy-MM-dd"), "MM")
    )
}
