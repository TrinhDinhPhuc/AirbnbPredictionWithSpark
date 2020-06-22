import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, types}
object ReadData {
    //csv files
  val PATH_LISTINGS_DETAIL = "/home/harry/Documents/Airbnb/Airbnb_Japan/listings.csv"
//  +--------------------+--------------------+--------------------+------------+--------------------+--------------------+--------------------+--------------------+-------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+----------+--------------------+--------------+-------+--------------------+---------+----------+--------------------+--------------------+------------------+------------------+--------------------+-----------------+------------------+----------------+------------------+-------------------+-------------------------+------------------+--------------------+----------------------+------+-------------+----------------------+----------------------------+----+-----+-------+------+--------------+------------+-------+--------+---------+-----------------+-------------+---------+------------+---------+--------+----+--------+---------+-----------+-----+------------+-------------+----------------+------------+---------------+------------+--------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------+----------------+---------------+---------------+---------------+----------------+---------------------+-----------------+---------------------+------------+-----------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+----------------+-------+------------------+----------------+------------------------+-------------------+-----------------------------+--------------------------------+------------------------------+-------------------------------------------+--------------------------------------------+-------------------------------------------+-----------------+
//  |                  id|         listing_url|           scrape_id|last_scraped|                name|             summary|               space|         description|experiences_offered|neighborhood_overview|               notes|             transit|              access|         interaction|         house_rules|thumbnail_url|medium_url|         picture_url|xl_picture_url|host_id|            host_url|host_name|host_since|       host_location|          host_about|host_response_time|host_response_rate|host_acceptance_rate|host_is_superhost|host_thumbnail_url|host_picture_url|host_neighbourhood|host_listings_count|host_total_listings_count|host_verifications|host_has_profile_pic|host_identity_verified|street|neighbourhood|neighbourhood_cleansed|neighbourhood_group_cleansed|city|state|zipcode|market|smart_location|country_code|country|latitude|longitude|is_location_exact|property_type|room_type|accommodates|bathrooms|bedrooms|beds|bed_type|amenities|square_feet|price|weekly_price|monthly_price|security_deposit|cleaning_fee|guests_included|extra_people|minimum_nights|maximum_nights|minimum_minimum_nights|maximum_minimum_nights|minimum_maximum_nights|maximum_maximum_nights|minimum_nights_avg_ntm|maximum_nights_avg_ntm|calendar_updated|has_availability|availability_30|availability_60|availability_90|availability_365|calendar_last_scraped|number_of_reviews|number_of_reviews_ltm|first_review|last_review|review_scores_rating|review_scores_accuracy|review_scores_cleanliness|review_scores_checkin|review_scores_communication|review_scores_location|review_scores_value|requires_license|license|jurisdiction_names|instant_bookable|is_business_travel_ready|cancellation_policy|require_guest_profile_picture|require_guest_phone_verification|calculated_host_listings_count|calculated_host_listings_count_entire_homes|calculated_host_listings_count_private_rooms|calculated_host_listings_count_shared_rooms|reviews_per_month|
//  +--------------------+--------------------+--------------------+------------+--------------------+--------------------+--------------------+--------------------+-------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+----------+--------------------+--------------+-------+--------------------+---------+----------+--------------------+--------------------+------------------+------------------+--------------------+-----------------+------------------+----------------+------------------+-------------------+-------------------------+------------------+--------------------+----------------------+------+-------------+----------------------+----------------------------+----+-----+-------+------+--------------+------------+-------+--------+---------+-----------------+-------------+---------+------------+---------+--------+----+--------+---------+-----------+-----+------------+-------------+----------------+------------+---------------+------------+--------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------+----------------+---------------+---------------+---------------+----------------+---------------------+-----------------+---------------------+------------+-----------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+----------------+-------+------------------+----------------+------------------------+-------------------+-----------------------------+--------------------------------+------------------------------+-------------------------------------------+--------------------------------------------+-------------------------------------------+-----------------+
//  |               35303|https://www.airbn...|      20200428053647|  2020-04-28|La Casa Gaienmae ...|This shared flat ...|This apartment is...|This shared flat ...|               none| 10 min walking to...|Current tenants a...|5min to subway, 1...|Your private room...|I provide common ...|If you would like...|         null|      null|https://a0.muscac...|          null| 151977|https://www.airbn...|   Miyuki|2010-06-25|Shibuya, Tokyo, J...|Hi I am Miyuki Ka...|              null|              null|                null|             null|              null|            null|              null|               null|                     null|              null|                null|                  null|  null|         null|                  null|                        null|null| null|   null|  null|          null|        null|   null|    null|     null|             null|         null|     null|        null|     null|    null|null|    null|     null|       null| null|        null|         null|            null|        null|           null|        null|          null|          null|                  null|                  null|                  null|                  null|                  null|                  null|            null|            null|           null|           null|           null|            null|                 null|             null|                 null|        null|       null|                null|                  null|                     null|                 null|                       null|                  null|               null|            null|   null|              null|            null|                    null|               null|                         null|                            null|                          null|                                       null|                                        null|                                       null|             null|
//  |I have been using...|                food| wine and cheeese!  |        null|                null|                null|                null|                null|               null|                 null|                null|                null|                null|                null|                null|         null|      null|                null|          null|   null|                null|     null|      null|                null|                null|              null|              null|                null|             null|              null|            null|              null|               null|                     null|              null|                null|                  null|  null|         null|                  null|                        null|null| null|   null|  null|          null|        null|   null|    null|     null|             null|         null|     null|        null|     null|    null|null|    null|     null|       null| null|        null|         null|            null|        null|           null|        null|          null|          null|                  null|                  null|                  null|                  null|                  null|                  null|            null|            null|           null|           null|           null|            null|                 null|             null|                 null|        null|       null|                null|                  null|                     null|                 null|                       null|                  null|               null|            null|   null|              null|            null|                    null|               null|                         null|                            null|                          null|                                       null|                                        null|                                       null|             null|
//  +--------------------+--------------------+--------------------+------------+--------------------+--------------------+--------------------+--------------------+-------------------+---------------------+--------------------+--------------------+--------------------+--------------------+--------------------+-------------+----------+--------------------+--------------+-------+--------------------+---------+----------+--------------------+--------------------+------------------+------------------+--------------------+-----------------+------------------+----------------+------------------+-------------------+-------------------------+------------------+--------------------+----------------------+------+-------------+----------------------+----------------------------+----+-----+-------+------+--------------+------------+-------+--------+---------+-----------------+-------------+---------+------------+---------+--------+----+--------+---------+-----------+-----+------------+-------------+----------------+------------+---------------+------------+--------------+--------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------+----------------+---------------+---------------+---------------+----------------+---------------------+-----------------+---------------------+------------+-----------+--------------------+----------------------+-------------------------+---------------------+---------------------------+----------------------+-------------------+----------------+-------+------------------+----------------+------------------------+-------------------+-----------------------------+--------------------------------+------------------------------+-------------------------------------------+--------------------------------------------+-------------------------------------------+-----------------+
//  only showing top 2 rows
  val PATH_LISTINGS = "/home/harry/Documents/Airbnb/Airbnb_Japan/listings_summary.csv"
//    +------+--------------------+-------+---------------+-------------------+-------------+--------+---------+---------------+-----+--------------+-----------------+-----------+-----------------+------------------------------+----------------+
//    |    id|                name|host_id|      host_name|neighbourhood_group|neighbourhood|latitude|longitude|      room_type|price|minimum_nights|number_of_reviews|last_review|reviews_per_month|calculated_host_listings_count|availability_365|
//    +------+--------------------+-------+---------------+-------------------+-------------+--------+---------+---------------+-----+--------------+-----------------+-----------+-----------------+------------------------------+----------------+
//    | 35303|La Casa Gaienmae ...| 151977|         Miyuki|               null|   Shibuya Ku|35.67152|139.71203|   Private room| 4183|            28|               18| 2018-07-28|             0.18|                             3|              89|
//    |197677|Oshiage Holiday A...| 964081|Yoshimi & Marek|               null|    Sumida Ku|35.71721|139.82596|Entire home/apt|11048|             3|              165| 2020-03-04|             1.57|                             1|             271|
//    +------+--------------------+-------+---------------+-------------------+-------------+--------+---------+---------------+-----+--------------+-----------------+-----------+-----------------+------------------------------+----------------+
//  only showing top 2 rows
  val PATH_NEIGHBOURHOOD = "/home/harry/Documents/Airbnb/Airbnb_Japan/neighbourhoods.csv"
//    +-------------------+-------------+
//    |neighbourhood_group|neighbourhood|
//    +-------------------+-------------+
//    |               null|    Adachi Ku|
//    |               null|  Akiruno Shi|
//    +-------------------+-------------+
//  only showing top 2 rows
  val PATH_REVIEWS_DETAIL = "/home/harry/Documents/Airbnb/Airbnb_Japan/reviews.csv"
//  +--------------------+--------------------+----------+-----------+-------------+--------------------+
//  |          listing_id|                  id|      date|reviewer_id|reviewer_name|            comments|
//  +--------------------+--------------------+----------+-----------+-------------+--------------------+
//  |               35303|              810980|2011-12-28|    1502908|        Firuz|Miyuki's has been...|
//  |Her place is very...|Harajuku stn and ...|      null|       null|         null|                null|
//  +--------------------+--------------------+----------+-----------+-------------+--------------------+
//  only showing top 2 rows
  val PATH_REVIEWS = "/home/harry/Documents/Airbnb/Airbnb_Japan/reviews_summary.csv"
//    +----------+-------------------+
//    |listing_id|               date|
//    +----------+-------------------+
//    |     35303|2011-12-28 00:00:00|
//    |     35303|2012-10-01 00:00:00|
//    +----------+-------------------+
//  only showing top 2 rows

  def readFullCSV_DF(sparkSession: SparkSession):DataFrame={
    val data = sparkSession
      .read
      .option("inferSchema","true")
      .option("header","true")
      .csv(PATH_LISTINGS_DETAIL)
    println(data.count()) //show no. rows
    println(data.columns.size) //show no. cols
    data.show(2)
    import org.apache.spark.sql.functions.{isnan, isnull, count, col}
    data.select()
    data.printSchema()

    data
  }

  def loadReviewsDetail(sparksession:  SparkSession): DataFrame = {
    val reviewsDetailSchema = StructType(Seq(
      StructField(name ="listing_id",dataType = IntegerType,nullable = false),
      StructField(name ="id",dataType = IntegerType,nullable = false),
      StructField(name ="date",dataType = StringType,nullable = false),
      StructField(name ="reviewer_id",dataType = IntegerType,nullable = false),
      StructField(name ="reviewer_name",dataType = StringType,nullable = false),
      StructField(name ="comments",dataType = DoubleType,nullable = false)
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
//    reviewsDetailDF.show(15)
        .filter(col("reviewer_id").isNotNull && col("reviewer_name").isNotNull )

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

  def loadListings(sparkession : SparkSession) : DataFrame = {

    val listingSchema = StructType(Seq(
      StructField(name = "id",dataType = IntegerType, nullable = false),
      StructField(name = "name",dataType = StringType, nullable = false),
      StructField(name = "host_id",dataType = IntegerType, nullable = false),
      StructField(name = "host_name",dataType = StringType, nullable = false),
      StructField(name = "neighbourhood_group", dataType = StringType, nullable = true),
      StructField(name = "neighbourhood",dataType = StringType, nullable = false),
      StructField(name = "latitude",dataType = FloatType, nullable = true),
      StructField(name = "longitude",dataType = FloatType, nullable = true),
      StructField(name = "room_type",dataType = StringType, nullable = true),
      StructField(name = "price",dataType = IntegerType, nullable = false),
      StructField(name = "minimum_nights",dataType = IntegerType, nullable = true),
      StructField(name = "number_of_reviews", dataType = IntegerType, nullable = true),
      StructField(name = "last_review", dataType = StringType, nullable = false),
      StructField(name = "reviews_per_month", dataType = FloatType, nullable = false),
      StructField(name = "calculated_host_listings_count",dataType = IntegerType, nullable = true),
      StructField(name = "availability_365", dataType = IntegerType, nullable = true)
    ))

    val listingsDF = sparkession
      .read
      .schema(listingSchema)
      .format("csv")
      .option("header","true")
      .option("mode", "DROPMALFORMED")
      .load(PATH_LISTINGS)
      .select(col (colName = "id"), col(colName = "host_id"), col(colName = "host_name"), col(colName = "neighbourhood"))
      .as(alias = "listingsDF")  //an alias set (equivalent to SQL "AS" keyword)
//    listingsDF.show(5)

//    +------+-------+-------------------+-------------+
//    |    id|host_id|          host_name|neighbourhood|
//    +------+-------+-------------------+-------------+
//    | 35303| 151977|             Miyuki|   Shibuya Ku|
//    |197677| 964081|    Yoshimi & Marek|    Sumida Ku|
//    |289597| 341577|           Hide&Kei|    Nerima Ku|
//    |370759|1573631|Gilles,Mayumi,Taiki|  Setagaya Ku|
//    |700253| 341577|           Hide&Kei|    Nerima Ku|
//    +------+-------+-------------------+-------------+
//    only showing top 5 rows
    listingsDF
  }

  def loadNeighbourhoods(spark: SparkSession): DataFrame = {

    val neighbourhoodsSchema = StructType(Seq(
      StructField("neighbourhood_group", StringType, true),
      StructField("neighbourhood", StringType, false)
    ))

    val neigbourhoodsDf = spark
      .read
      .schema(neighbourhoodsSchema)
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(PATH_NEIGHBOURHOOD)
      .drop("neighbourhood_group")
      .withColumn("neighbourhood_id", monotonically_increasing_id())
      .as("neigbourhoodsDf")
//    neigbourhoodsDf.show(5)
//      +--------------+----------------+
//      | neighbourhood|neighbourhood_id|
//      +--------------+----------------+
//      |     Adachi Ku|               0|
//      |   Akiruno Shi|               1|
//      |  Akishima Shi|               2|
//      |Aogashima Mura|               3|
//      |    Arakawa Ku|               4|
//      +--------------+----------------+
//    only showing top 5 rows
    neigbourhoodsDf
  }

  // 4. neighbourhood_id(Long) -> neighbourhood_name(String) dictionary
    def getNeighbourhoodMap(sparkSession: SparkSession, neighbourhoodDF : DataFrame): Unit ={
      import sparkSession.implicits._
      val neighbourhoodMap = neighbourhoodDF
        .select(col(colName = "neighbourhood_id"), col(colName = "neighbourhood"))
        .as[(Long,String)]
        .collect()
        .toMap
      neighbourhoodMap
    }

  def getReviewerMap(sparkSession: SparkSession, reviewsDetailDF:DataFrame): Unit ={
    import sparkSession.implicits._
    val reviewerMap = reviewsDetailDF
      .select(col(colName = "reviewer_id"),col(colName = "reviewer_name"))
      .as[(Long, String)]
    //      +-----------+-------------+
    //      |reviewer_id|reviewer_name|
    //      +-----------+-------------+
    //      |    1502908|        Firuz|
    //      |       null|         null|
    //      |       null|         null|
    //      |       null|         null|
    //      +-----------+-------------+
      .collect().foreach(println)
//      .toMap
//    reviewerMap.show(4)
    reviewerMap

  }
}
