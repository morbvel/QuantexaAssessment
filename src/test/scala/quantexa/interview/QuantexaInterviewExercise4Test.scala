package quantexa.interview

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewExercise4Test extends SparkTesting{

  import spark.implicits._

  "getRenamedPassengers" should "rename and return the passengers column renamed" in {

    val inputDf = List(
      ("1", "passenger1", "1"), ("2", "passenger2", "2"), ("3", "passenger3", "3")
    ).toDF("id", "passengerId", "flightId")

    val results = QuantexaInterviewExercise4.getRenamedPassengers(inputDf)

    assert("passenger1" == getValueFromId[String](results, 1, "passengerId2"))
    assert("passenger2" == getValueFromId[String](results, 2, "passengerId2"))
    assert("passenger3" == getValueFromId[String](results, 3, "passengerId2"))
  }

  "getInnerPassengers" should "join based on the flightId" in {

    val inputDf1 = List(
      ("1", "1", "1", "2023-01-01"),
      ("2", "4", "1", "2023-01-01"),
      ("3", "7", "1", "2023-01-01")
    ).toDF("id", "passengerId", "flightId", "date")

    val inputDf2 = List(
      ("1", "1"),
      ("2", "1"),
      ("6", "1")
    ).toDF("passengerId2", "flightId")

    val results = inputDf1.transform(QuantexaInterviewExercise4.getInnerPassengers(inputDf2))
        .withColumn("id", row_number.over(Window.partitionBy("flightId").orderBy("date")))

    assert("1" == getValueFromId[String](results, 1, "passengerId"))
    assert("6" == getValueFromId[String](results, 1, "passengerId2"))
    assert("1" == getValueFromId[String](results, 2, "passengerId"))
    assert("2" == getValueFromId[String](results, 2, "passengerId2"))
    assert("4" == getValueFromId[String](results, 3, "passengerId"))
    assert("6" == getValueFromId[String](results, 3, "passengerId2"))
    assert("4" == getValueFromId[String](results, 4, "passengerId"))
    assert("2" == getValueFromId[String](results, 4, "passengerId2"))
    assert("4" == getValueFromId[String](results, 5, "passengerId"))
    assert("1" == getValueFromId[String](results, 5, "passengerId2"))
    assert("7" == getValueFromId[String](results, 6, "passengerId"))
    assert("6" == getValueFromId[String](results, 6, "passengerId2"))
    assert("7" == getValueFromId[String](results, 7, "passengerId"))
    assert("2" == getValueFromId[String](results, 7, "passengerId2"))
    assert("7" == getValueFromId[String](results, 8, "passengerId"))
    assert("1" == getValueFromId[String](results, 8, "passengerId2"))
  }

  "getNumberOfFlightsTogether" should "return the number of flights together for passengers" in {

    val inputDf: DataFrame = List(
      ("1", "2", "1", "2023-01-01"),
      ("1", "5", "2", "2023-01-01"),
      ("1", "5", "1", "2023-01-03"),
    ).toDF("passengerId", "passengerId2", "flightId", "date")

    val results = QuantexaInterviewExercise4.getNumberOfFlightsTogether(inputDf)

    assert(1 == results
      .filter(col("passengerId").equalTo("1") && col("passengerId2").equalTo("2"))
      .head.getAs[Long]("Number of flights together")
    )
    assert(2 == results
      .filter(col("passengerId").equalTo("1") && col("passengerId2").equalTo("5"))
      .head.getAs[Long]("Number of flights together")
    )
  }

  "getPassengersInFlightsTogether" should "return the ammount of flights together for passengers" in {

    val flights: DataFrame = List(
      ("1", "1", "dummy"), ("1", "2", "dummy"), ("1", "3", "dummy"), ("1", "4", "dummy"),
      ("1", "5", "dummy"), ("1", "6", "dummy"), ("1", "7", "dummy"), ("1", "8", "dummy"), ("1", "9", "dummy"),
      ("2", "1", "dummy"), ("2", "2", "dummy"), ("2", "3", "dummy"), ("2", "4", "dummy"),
      ("3", "6", "dummy"), ("3", "7", "dummy"), ("3", "8", "dummy"), ("3", "9", "dummy")
    ).toDF("passengerId","flightId", "date")

    val results = QuantexaInterviewExercise4.getPassengersInFlightsTogether(flights, 3)

    val groupedByPassenger = results
      .groupBy("passengerId").count()
      .withColumn("id", col("passengerId"))

    val colToTest = "count"
    assert(2 == getValueFromId[Long](groupedByPassenger, 1, colToTest))
    assert(1 == getValueFromId[Long](groupedByPassenger, 2, colToTest))
    assert(1 == getValueFromId[Long](groupedByPassenger, 3, colToTest))
  }
}
