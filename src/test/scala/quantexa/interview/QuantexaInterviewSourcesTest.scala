package quantexa.interview

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import quantexa.interview.{QuantexaInterviewSources, SparkTesting}

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewSourcesTest extends SparkTesting {

  "getSourceData" should "read and create a dataframe for the PASSANGERS table" in {
    val expectedResults: Set[Row] = Set(
      Row("1", "firstName1", "lastName1"),
      Row("2", "firstName2", "lastName2"),
      Row("3", "firstName3", "lastName3")
    )

    val expectedColumns: List[String] = List("passengerId","firstName","lastName")
    val pathToTest = "/passangers_test.csv"

    val results = QuantexaInterviewSources.getSourceData(pathToTest, "passangers")

    assert( expectedResults.size == results.count() )
    assert( expectedResults == results.collect().toSet )
    assert( expectedColumns == results.columns.toList )
  }

  it should "read and create a dataframe for the FLIGHTS table" in {
    val expectedResults: Set[Row] = Set(
      Row("1", "1", "fromTest1", "toTest1", "2020-01-01"),
      Row("2", "1", "fromTest1", "toTest1", "2020-01-01"),
      Row("3", "1", "fromTest1", "toTest1", "2020-01-01")
    )

    val expectedColumns: List[String] = List("passengerId", "flightId", "from", "to", "date")
    val pathToTest = "/flightData_test.csv"

    val results = QuantexaInterviewSources.getSourceData(pathToTest, "flights")

    assert( expectedResults.size == results.count() )
    assert( expectedResults == results.collect().toSet )
    assert( expectedColumns == results.columns.toList )
  }

}
