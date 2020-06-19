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
//    val readFullData = ReadData.readFullCSV_DF(sparkSession)
    val reviewsDetailDF = ReadData.loadReviewsDetail(sparkSession)
    val reviewsDetailDF_cleaned = EDA.reviewsDetailDF_EDA(sparkSession,reviewsDetailDF)
    System.exit(1)
    val listingsDF = ReadData.loadListings(sparkSession)
    val neighbourhoodDF = ReadData.loadNeighbourhoods(sparkSession)

    //Map0
    val neighbourhoodMap:Unit = ReadData.getNeighbourhoodMap(sparkSession, neighbourhoodDF)
    //A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable cached on each machine
    // rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset
    // in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms
    // to reduce communication cost.
    sparkSession.sparkContext.broadcast(neighbourhoodMap)

    val reviewerMap = ReadData.getReviewerMap(sparkSession, reviewsDetailDF)
    sparkSession.sparkContext.broadcast(reviewerMap)

//    val rating = Recommender.getRating(sparkSession, listingsDF: DataFrame, neighbourhoodDF: DataFrame, reviewsDetailDF: DataFrame)

  }
}
