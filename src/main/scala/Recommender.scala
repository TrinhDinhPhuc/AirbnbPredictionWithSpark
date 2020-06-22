import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}
object Recommender {

  def getRating(sparkSession: SparkSession, listingsDf : DataFrame, neibourhoodDf : DataFrame, reviewsDetailDf : DataFrame): Unit ={
    import sparkSession.implicits._

    // listings
    val joinedListingNeighbourDf = listingsDf
      .join(neibourhoodDf, col(colName = "listingsDf.neighbourhood") === col(colName = "neigbourhoodsDf.neighbourhood"),joinType = "inner")
      .drop(col(colName = "neigbourhoodsDf.neighbourhood"))
      .as(alias = "joinedListingNeighbourDf")

    joinedListingNeighbourDf.show(5)

//      +------+-------+-------------------+-------------+----------------+
//      |    id|host_id|          host_name|neighbourhood|neighbourhood_id|
//      +------+-------+-------------------+-------------+----------------+
//      | 35303| 151977|             Miyuki|   Shibuya Ku|              52|
//      |197677| 964081|    Yoshimi & Marek|    Sumida Ku|              56|
//      |289597| 341577|           Hide&Kei|    Nerima Ku|              43|
//      |370759|1573631|Gilles,Mayumi,Taiki|  Setagaya Ku|              51|
//      |700253| 341577|           Hide&Kei|    Nerima Ku|              43|
//      +------+-------+-------------------+-------------+----------------+
//    only showing top 5 rows
    print(reviewsDetailDf.printSchema())
    val joinedListingReviewsDf = joinedListingNeighbourDf
      .join(reviewsDetailDf, col("joinedListingNeighbourDf.id") === col("reviewsDetailDf.listing_id"), "inner")
      .drop("id")
      .as("joinedListingReviewsDf")
    joinedListingReviewsDf.show(5)
    println(s">> joinedListingReviewsDf count: ${joinedListingReviewsDf.count()}")
//      +--------+---------+-------------+----------------+----------+----------+-----------+-------------+
//      | host_id|host_name|neighbourhood|neighbourhood_id|listing_id|      date|reviewer_id|reviewer_name|
//      +--------+---------+-------------+----------------+----------+----------+-----------+-------------+
//      |19152993|      Sei|      Kita Ku|              24|   4888140|2015-02-23|   27196217|      Sujitra|
//      |19152993|      Sei|      Kita Ku|              24|   4888140|2015-02-27|   24716396|      Michael|
//      |19152993|      Sei|      Kita Ku|              24|   4888140|2015-03-20|   27693465|        Cyrus|
//      |19152993|      Sei|      Kita Ku|              24|   4888140|2015-03-30|   25040486|     Angelica|
//      |19152993|      Sei|      Kita Ku|              24|   4888140|2015-04-04|   26105293|         Alex|
//      +--------+---------+-------------+----------------+----------+----------+-----------+-------------+
//    only showing top 5 rows



  }
}
