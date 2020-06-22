import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{when}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object EDA {
  def reviewsDetailDF_EDA(sparkSession: SparkSession,reviewsDetailDF:DataFrame): Unit ={
    println("isNull: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id").isNull ).count())
    println("isNaN: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id").isNaN).count())
    println("blank: " + reviewsDetailDF.filter(reviewsDetailDF("reviewer_id") === "").count())
    reviewsDetailDF.filter(col("reviewer_id").isNotNull && col("reviewer_name").isNotNull ).show()
    println(reviewsDetailDF.filter(col("reviewer_id").isNotNull && col("reviewer_name").isNotNull ).count())
//    reviewsDetailDF.createGlobalTempView("reviewer_name")
//    sparkSession.sql("select  * from reviewsDetailDF.reviewer_name").show()
//    reviewsDetailDF.filter("reviewer_id is null").show()
  }
}
