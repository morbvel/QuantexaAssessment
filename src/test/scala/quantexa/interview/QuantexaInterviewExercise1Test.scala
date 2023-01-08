package quantexa.interview

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewExercise1Test extends SparkTesting {
  import spark.implicits._

  "getMonthValueColumn" should "return the month value given the date" in {
    val inputDf = List(
      (1, "2020-01-01"),
      (2, "2020-02-01"),
      (3, "2020-12-01"),
      (4, "2020-07-01")
    ).toDF("id", "date")

    val result = inputDf.transform(QuantexaInterviewExercise1.getMonthValueColumn)

    val columnToTest = "month"

    assert( "01" == getValueFromId[String](result, 1, columnToTest) )
    assert( "02" == getValueFromId[String](result, 2, columnToTest) )
    assert( "12" == getValueFromId[String](result, 3, columnToTest) )
    assert( "07" == getValueFromId[String](result, 4, columnToTest) )

  }

  "getFlightsPerMonth" should "return the actual number of flights per month" in {
    val inputDf = List(
      ("2020-01-01"),
      ("2020-01-01"),
      ("2020-07-01"),
      ("2020-07-01"),
      ("2020-07-01"),
      ("2020-10-01"),
      ("2020-12-01"),
      ("2020-12-01")
    ).toDF("date")

    val expectedResults = Set(
      Row("01", 2),
      Row("07", 3),
      Row("10", 1),
      Row("12", 2)
    )

    val results = inputDf.transform(QuantexaInterviewExercise1.getFlightsPerMonth)

    assert(results.count == expectedResults.size)
    assert(results.collect().toSet == expectedResults)
  }
}
