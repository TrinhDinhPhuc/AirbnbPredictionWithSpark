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
      .join(neibourhoodDf, col(colName = "listingsDf.neighbourhood") === col(colName = "neighbourhoodsDf.neibourhood"),joinType = "inner")
      .drop(col(colName = "neighbourhoodsDf.neighbourhood"))
      .as(alias = "joinedListingNeighbourDf")

    joinedListingNeighbourDf.show(5)
  }
}
