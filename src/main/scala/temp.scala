import org.apache.spark.sql.{SparkSession,SaveMode,DataFrame}
import  org.apache.spark.{SparkContext, SparkConf}

object temp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("AirbnbRecommender_Projcet")
      .config("spark.driver.bindAddress","127.0.0.1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffet.max","128m")
      .config("spark.evenLog.enabled","true")
      .getOrCreate()

    val myRange = sparkSession.range(1000)
    println(myRange.getClass().getName()) //get variable's type (similar to Java)

    // Encoders for most common types are automatically provided by importing spark.implicits._
    import sparkSession.implicits._

    val peopleDF = sparkSession.read.json("/home/harry/Documents/Airbnb/toyData/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = sparkSession.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = sparkSession.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
  }
}
