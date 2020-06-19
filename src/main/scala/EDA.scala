import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object EDA {
  def reviewsDetailDF_EDA(sparkSession: SparkSession,reviewsDetailDF:DataFrame): Unit ={
    println(reviewsDetailDF.filter(reviewsDetailDF("reviewer_id").isNull || reviewsDetailDF("reviewer_id") === "" || reviewsDetailDF("reviewer_id").isNaN).count())

  }
}
