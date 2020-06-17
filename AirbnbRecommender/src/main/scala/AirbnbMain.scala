import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AirbnbMain {

  val PATH_ALS_MODEL = "/home/harry/Documents/Airbnb/Airbnb_Japan/model"
  val PATH_RESULT_PARQUET = "/home/harry/Documents/Airbnb/Airbnb_Japan/resultDf.parquet"

  def main(args: Array[String]): Unit = {
    //SparkSession
//    val spark = SparkSession
//      .builder()
//      .master("spark://220.149.84.24:7077")
//      .appName("AirbnbRecommender")
//      .config("spark.driver.bindAddress", "127.0.0.1")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.kryoserializer.buffer.max", "128m")
//      .config("spark.eventLog.enabled", "true")
//      .getOrCreate()
    // SparkContext
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]") //spark://220.149.84.24:7077
    conf.setAppName("AirbnbRecommender")
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max", "128m")
    conf.set("spark.eventLog.enabled", "true")

    val sc = new SparkContext(conf)

    // SparkSession
    val spark = SparkSession
      .builder()
      .appName("AirbnbRecommender")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

     //load dataframe
    val reviewsDetailDf = Loader.loadReviewsDetail(spark)
    val listingsDf = Loader.loadListings(spark)
    val neigbourhoodsDf = Loader.loadNeighbourhoods(spark)

    // id2name Map0
    val neighbourhoodMap = Loader.getNeighbourhoodMap(spark, neigbourhoodsDf)
    val reviewerMap = Loader.getReviewerMap(spark, reviewsDetailDf)
    sc.broadcast(neighbourhoodMap)  // broadcast to executors
    sc.broadcast(reviewerMap)       // broadcast to executors

    // ALS recommendations
    val rating = Recommender.getRating(spark, listingsDf: DataFrame, neigbourhoodsDf: DataFrame, reviewsDetailDf: DataFrame)
    val mse = Recommender.trainModel(sc, rating, 3, PATH_ALS_MODEL)

    // >> Mean Squared Error = 0.003322032007912435
    println(">> Mean Squared Error = " + mse)

    // load model and make recommendations
    val loadedModel = Recommender.loadModel(sc, PATH_ALS_MODEL)
    val recommendations = Recommender.getRecommendations(spark, loadedModel, 5, reviewerMap, neighbourhoodMap)

    // 최종 결과 DataFrame을 parquet 형태로 저장 (실제 서비스에선 mysql등 db에 저장해야 함)
    recommendations.write.mode(saveMode = SaveMode.Overwrite).parquet(PATH_RESULT_PARQUET)

    // 결과 출력
    recommendations.show(10, false)
  }
}
