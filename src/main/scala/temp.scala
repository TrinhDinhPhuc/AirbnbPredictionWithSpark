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

  }
}
