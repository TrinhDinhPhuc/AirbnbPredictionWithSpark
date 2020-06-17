import scala.xml._
import scala.xml.XML
import java.io.File

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object readXML {
  def main(args: Array[String]): Unit = {
    //List all files in a directory
    val stackDirectory = "/media/harry/harry/torrent/stackexchange_data"
    // list only the folders directly under this directory (does not recurse)
    val folders: Array[File] = (new File(stackDirectory))
      .listFiles()
    folders.foreach(println)


    //Create Spark Session
    val spark = SparkSession
      .builder()
      .master("spark://220.149.84.24:7077")
      .appName("MyAppChafo m")
      .config("spark.some.config.option", "some-value")
      .config("spark.submit.deployMode","cluster")
      .getOrCreate()

  }
}