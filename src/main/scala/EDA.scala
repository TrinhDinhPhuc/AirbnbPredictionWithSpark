import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object EDA {
  def reviewsDetailDF_EDA(sparkSession: SparkSession,reviewsDetailDF:DataFrame): Unit ={
    println("isNull: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id").isNull ).count())
    println("isNaN: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id").isNaN).count())
    println("blank: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id") === "").count())
    println(reviewsDetailDF.count())
//    reviewsDetailDF.na.fill(0)
    reviewsDetailDF.drop(when($"id".isNull, 0).otherwise(1)).show

    println(reviewsDetailDF.count())
    reviewsDetailDF.filter("reviewer_id is null").show()
    reviewsDetailDF.filter($"reviewer_id".isNotNull)
  }
}
