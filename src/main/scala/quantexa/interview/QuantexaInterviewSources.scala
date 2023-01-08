package quantexa.interview

import org.apache.spark.sql.{DataFrame, SparkSession}

object QuantexaInterviewSources extends Constants {

  case class Passangers(passengerId: String, firstName: String, lastName: String)
  case class FlightData(passengerId: String, flightId: String, from: String, to: String, date: String)

  def getSourceData(filePath: String, fileName: String)(implicit spark: SparkSession): DataFrame = {
    val localFileStream = getClass.getResourceAsStream(filePath)

    val data = scala.io.Source.fromInputStream(localFileStream).getLines().toSeq.tail

    val passangersDataframe: DataFrame = fileName match {
      case PASSANGERS_TABLE => getPassangersRawData(data)
      case _ => null
    }

    val flightsDataframe: DataFrame = fileName match {
      case FLIGHTS_TABLE => getFlightsRawData(data)
      case _ => null
    }

    if(passangersDataframe != null) passangersDataframe
    else if(flightsDataframe != null) flightsDataframe
    else throw new Exception("No Dataframe could be created for the path provided")

  }

  def getPassangersRawData(dataSequence: Seq[String])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    dataSequence.map{
      item: String => {
        val values = item.split(",")
        Passangers(values(0), values(1), values(2))
      }
    }.toDF()
  }

  def getFlightsRawData(dataSequence: Seq[String])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    dataSequence.map{
      item: String => {
        val values = item.split(",")
        FlightData(values(0), values(1), values(2), values(3), values(4))
      }
    }.toDF()
  }


}
