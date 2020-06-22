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
    val mse = Recommender.trainModel(sparkSession,rating,numIterations = 3, PATH_ALS_MODEL)

    // >> Mean Squared Error = 0.003322032007912435
    println(">> Mean Squared Error = " + mse)

    // load model and make recommendations
    val loadedModel = Recommender.loadModel(sparkSession, PATH_ALS_MODEL)
    val recommendations = Recommender.getRecommendations(sparkSession , loadedModel, 5, reviewerMap, neighbourhoodMap)

    // Store final result DataFrame in parquet format (mysql lamp db should be stored in actual service)
    recommendations.write.mode(saveMode = SaveMode.Overwrite).parquet(PATH_RESULT_PARQUET)

    // output result
    recommendations.show(10, false)

  }
}
