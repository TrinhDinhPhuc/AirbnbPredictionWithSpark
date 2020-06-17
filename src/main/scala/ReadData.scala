import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, types}
object ReadData {
    //csv files
  val PATH_LISTINGS_DETAIL = "/home/harry/Documents/Airbnb/Airbnb_Japan/listings.csv"
  val PATH_LISTINGS = "/home/harry/Documents/Airbnb/Airbnb_Japan/listings_summary.csv"
  val PATH_NEIGHBOURHOOD = "/home/harry/Documents/Airbnb/Airbnb_Japan/neighbourhoods.csv"
  val PATH_REVIEWS_DETAIL = "/home/harry/Documents/Airbnb/Airbnb_Japan/reviews.csv"
  val PATH_REVIEWS = "/home/harry/Documents/Airbnb/Airbnb_Japan/reviews_summary.csv"

  def readFullCSV(sparkSession: SparkSession):DataFrame={
    val data = sparkSession
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv(PATH_REVIEWS_DETAIL)

    data.show(2)
    data
  }

  def loadReviewsDetail(sparksession:  SparkSession): DataFrame = {
    val reviewsDetailSchema = StructType(Seq(
      StructField("listing_id",IntegerType,false),
      StructField("id",IntegerType,false),
      StructField("date",StringType,false),
      StructField("reviewer_id",IntegerType,false),
      StructField("reviewer_name",StringType,false),
      StructField("comments",DoubleType,false)
    ))

    val reviewsDetailDF = sparksession
      .read
      .schema(reviewsDetailSchema)
      .format("csv")
      .option("header","true")
      .option("mode","DROPMALFORMED")
      .load(PATH_REVIEWS_DETAIL)
      .select(col("listing_id"), col("date"), col("reviewer_id"), col("reviewer_name"))
      .as("reviewsDetailDf")
    reviewsDetailDF.show(15)

  reviewsDetailDF
    //      +----------+----------+-----------+-------------+
    //      |listing_id|      date|reviewer_id|reviewer_name|
    //      +----------+----------+-----------+-------------+
    //      |     35303|2011-12-28|    1502908|        Firuz|
    //      |     35303|2012-10-01|     350719|       Jordan|
    //      |     35303|2013-02-18|    4917704|      Aymeric|
    //      |     35303|2013-03-30|    3243253|     Blandine|
    //      |     35303|2013-05-01|    1536097|     Kayleigh|
    //      |     35303|2013-05-14|    1822025|     Danielle|
    //      |     35303|2013-08-27|    6980559|       Tobias|
    //      |     35303|2014-09-08|    2893501|          Yan|
    //      |     35303|2014-11-24|    8674085|          San|
    //      |     35303|2015-03-03|   27944062|     Kim Seng|
    //      |     35303|2015-03-13|   28171041|         Matt|
    //      |     35303|2015-04-22|   17232501|        James|
    //      |     35303|2015-05-07|   25656628|       Marcus|
    //      |     35303|2015-05-15|   30292719|      Mohamed|
    //      |     35303|2015-05-26|   30190986|    Sebastien|
    //      +----------+----------+-----------+-------------+
    //    only showing top 15 rows

  }
}
