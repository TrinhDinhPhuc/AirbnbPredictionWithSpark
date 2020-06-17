import org.apache.spark.sql.{SparkSession,SaveMode,DataFrame}
import  org.apache.spark.{SparkContext, SparkConf}

object RecommenderMain {

  val PATH_ALS_MODEL = "/home/harry/Documents/Airbnb/Airbnb_Japan/model"
  val PATH_RESULT_PARQUET = "/home/harry/Documents/Airbnb/Airbnb_Japan/resultDf.parquet"
  def main(args: Array[String]):Unit ={
  //SparkSession
  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("AirbnbRecommender_Projcet")
    .config("spark.driver.bindAddress","127.0.0.1")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffet.max","128m")
    .config("spark.evenLog.enabled","true")
    .getOrCreate()

    //load dataframe
    val readFullData = ReadData.readFullCSV(sparkSession)
    val reviewsDetailDf = ReadData.loadReviewsDetail(sparkSession)


  }
}
