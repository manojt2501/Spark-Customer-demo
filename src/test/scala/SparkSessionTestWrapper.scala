import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

val spark: SparkSession = {
  SparkSession.builder()
  .master("local[1]")
  .appName("Local Test")
  .getOrCreate()
}
}
