package quantexa.interview

import java.nio.file.Paths

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.FlatSpec

trait SparkTesting extends FlatSpec{
  val path = Paths.get("").toAbsolutePath.toString + "/spark-warehouse"

  implicit val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", path) // fix for bug in windows
    .config("spark.ui.enabled", value = false)
    .config("spark.driver.host", "localhost")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .appName(getClass.getSimpleName)
    .enableHiveSupport()
    .getOrCreate()

  def getValueFromId[T](inputDf: DataFrame, id: Int, colToTest: String): T =
    inputDf.filter(col("id").equalTo(id)).head.getAs[T](colToTest)
}
