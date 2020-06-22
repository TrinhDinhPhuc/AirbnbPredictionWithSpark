import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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
//    val reviewsDetailDF_cleaned = EDA.reviewsDetailDF_EDA(sparkSession,reviewsDetailDF)
//    System.exit(1)
    val listingsDF = ReadData.loadListings(sparkSession)
    val neighbourhoodDF = ReadData.loadNeighbourhoods(sparkSession)

    //Map0
    val neighbourhoodMap = ReadData.getNeighbourhoodMap(sparkSession, neighbourhoodDF)
    //A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable cached on each machine
    // rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset
    // in an efficient manner. Spark also attempts to distribute broadcast variables using efficient broadcast algorithms
    // to reduce communication cost.
    sparkSession.sparkContext.broadcast(neighbourhoodMap)

    val reviewerMap = ReadData.getReviewerMap(sparkSession, reviewsDetailDF)
    sparkSession.sparkContext.broadcast(reviewerMap)

    val rating = Recommender.getRating(sparkSession, listingsDF: DataFrame, neighbourhoodDF: DataFrame, reviewsDetailDF: DataFrame)
    val mse = Recommender.trainModel(sparkSession, rating, numIterations = 3, PATH_ALS_MODEL)

    // >> Mean Squared Error = 0.003322032007912435
    println(">> Mean Squared Error = " + mse)

    // load model and make recommendations
    val loadedModel = Recommender.loadModel(sparkSession, PATH_ALS_MODEL)
    val recommendations = Recommender.getRecommendations(sparkSession , loadedModel, 5, reviewerMap, neighbourhoodMap)

    // Store final result DataFrame in parquet format (mysql lamp db should be stored in actual service)
    recommendations.write.mode(saveMode = SaveMode.Overwrite).parquet(PATH_RESULT_PARQUET)

    // output result
    recommendations.show(15, false)
//      +----------+------------+---------------------------------------------------------------------+----------+
//      |reviewerId|reviewerName|neighbourhoodNames                                                   |date      |
//      +----------+------------+---------------------------------------------------------------------+----------+
//      |6764076   |Chally      |[Akishima Shi, Taito Ku, Chuo Ku, Sumida Ku, Shinjuku Ku]            |2020-06-22|
//      |101965512 |Matthew     |[Shinjuku Ku, Taito Ku, Sumida Ku, Fussa Shi, Shibuya Ku]            |2020-06-22|
//      |246517788 |Ole         |[Hamura Shi, Sumida Ku, Shinjuku Ku, Taito Ku, Chuo Ku]              |2020-06-22|
//      |37084608  |Barbara     |[Taito Ku, Fussa Shi, Adachi Ku, Sumida Ku, Nakano Ku]               |2020-06-22|
//      |290292288 |Emil        |[Hamura Shi, Akishima Shi, Chuo Ku, Taito Ku, Higashiyamato Shi]     |2020-06-22|
//      |29066136  |Maggie      |[Hamura Shi, Akishima Shi, Minato Ku, Taito Ku, Ota Ku]              |2020-06-22|
//      |306054540 |대현         |[Hamura Shi, Sumida Ku, Shinjuku Ku, Taito Ku, Chuo Ku]              |2020-06-22|
//      |54444060  |Joseph      |[Hamura Shi, Sumida Ku, Shinjuku Ku, Taito Ku, Chuo Ku]              |2020-06-22|
//      |5491860   |Chiara      |[Toshima Ku, Taito Ku, Shinjuku Ku, Fuchu Shi, Fussa Shi]            |2020-06-22|
//      |52137756  |Jaxon       |[Shinjuku Ku, Taito Ku, Sumida Ku, Fussa Shi, Shibuya Ku]            |2020-06-22|
//      |206254092 |Richard     |[Taito Ku, Shinjuku Ku, Chuo Ku, Sumida Ku, Shibuya Ku]              |2020-06-22|
//      |164748804 |勇志         |[Hamura Shi, Sumida Ku, Shinjuku Ku, Taito Ku, Chuo Ku]              |2020-06-22|
//      |60849096  |Michael     |[Akishima Shi, Shibuya Ku, Taito Ku, Hamura Shi, Fussa Shi]          |2020-06-22|
//      |232039236 |丹          |[Toshima Ku, Taito Ku, Shinjuku Ku, Fuchu Shi, Fussa Shi]            |2020-06-22|
//      |98843844  |Debbie      |[Fussa Shi, Tachikawa Shi, Fuchu Shi, Suginami Ku, Higashikurume Shi]|2020-06-22|
//      |161606676 |Natthawut   |[Taito Ku, Shinjuku Ku, Chuo Ku, Sumida Ku, Shibuya Ku]              |2020-06-22|
//      +----------+------------+---------------------------------------------------------------------+----------+
//    only showing top 15 rows

  }
}
