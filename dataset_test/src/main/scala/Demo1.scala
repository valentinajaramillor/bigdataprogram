import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Demo1 extends App {

  val spark = SparkSession.builder()
    .appName("Demo")
    .config("spark.master", "local[*]")
    .getOrCreate()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")



  val serviceRequestsSchema = StructType(Array(
    StructField("unique_key", StringType),
    StructField("created_date", TimestampType),
    StructField("closed_date", TimestampType),
    StructField("agency", StringType),
    StructField("agency_name", StringType),
    StructField("complaint_type", StringType),
    StructField("descriptor", StringType),
    StructField("location_type", StringType),
    StructField("incident_zip", StringType),
    StructField("incident_address", StringType),
    StructField("street_name", StringType),
    StructField("cross_street_1", StringType),
    StructField("cross_street_2", StringType),
    StructField("intersection_street_1", StringType),
    StructField("intersection_street_2", StringType),
    StructField("address_type", StringType),
    StructField("city", StringType),
    StructField("landmark", StringType),
    StructField("facility_type", StringType),
    StructField("status", StringType),
    StructField("due_date", TimestampType),
    StructField("resolution_description", StringType),
    StructField("resolution_action_updated_date", TimestampType),
    StructField("community_board", StringType),
    StructField("bbl", StringType),
    StructField("borough", StringType),
    StructField("x_coordinate", DoubleType),
    StructField("y_coordinate", DoubleType),
    StructField("open_data_channel_type", StringType),
    StructField("park_facility_name", StringType),
    StructField("park_borough", StringType),
    StructField("vehicle_type", StringType),
    StructField("taxi_company_borough", StringType),
    StructField("taxi_pick_up_location", StringType),
    StructField("bridge_highway_name", StringType),
    StructField("bridge_highway_direction", StringType),
    StructField("road_ramp", StringType),
    StructField("bridge_highway_segment", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("location", StringType)
  ))

  val serviceRequestsDF = spark.read
    .schema(serviceRequestsSchema)
    .option("timestampFormat", "MM/dd/YYYY hh:mm:ss aa")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/311_Service_Requests_from_2010_to_Present.csv")


  val countRows = serviceRequestsDF.count()

  println(countRows)

  /**
   * Questions:
   *
   * 1. Which agencies have the highest number of service requests?
   * 2. What are the most recurrent types of complaints in the agencies with the most service requests?
   * 3. Which cities have the highest number of service requests?
   * 4. Which boroughs have the highest number of service requests?
   * 5. What are the most common channels through which service requests are made?
   * 6. How has the channel evolved in the last years?
   * 7. Which are the boroughs and zipcodes that have the highest number of noise complaints?
   * 8. Which are the boroughs and zipcodes that have the highest number of drug activity complaints?
   * 9. What is the hour of the day with the most service requests?
   * 10. What is the average resolution time by agency?
   */

  // Question 1
  val serviceRequestsByAgencyDF = serviceRequestsDF
    .groupBy(col("agency_name"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 2
  def serviceRequestsByAgencyByComplaintDF(agencyName: String) = serviceRequestsDF
    .where(col("agency_name") === agencyName)
    .groupBy(col("complaint_type"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  val serviceRequestsPoliceByComplaintDF = serviceRequestsByAgencyByComplaintDF("New York City Police Department")
  val serviceRequestsHousingByComplaintDF = serviceRequestsByAgencyByComplaintDF("Department of Housing Preservation and Development")
  val serviceRequestsTransportationByComplaintDF = serviceRequestsByAgencyByComplaintDF("Department of Transportation")

  // Question 3
  val serviceRequestsByCityDF = serviceRequestsDF
    .groupBy(col("city"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 4
  val serviceRequestsByBoroughDF = serviceRequestsDF
    .groupBy(col("borough"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 5
  val serviceRequestsByChannelDF = serviceRequestsDF
    .groupBy(col("open_data_channel_type"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 6
  val channelEvolutionDF = serviceRequestsDF
    .where(
      col("open_data_channel_type") === "PHONE" or
        col("open_data_channel_type") === "ONLINE" or
        col("open_data_channel_type") === "MOBILE" or
        col("open_data_channel_type") === "UNKNOWN" or
        col("open_data_channel_type") === "OTHER")
    .groupBy(
      year(col("created_date")).as("created_year"),
      col("open_data_channel_type"))
    .agg(count("*").as("count"))
    .orderBy(col("created_year"), col("count").desc)


  // Question 7
  val noiseComplaintsByBoroughDF = serviceRequestsDF
    .where(col("complaint_type").contains("Noise"))
    .groupBy(col("borough"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  val noiseComplaintsByZipcodeDF = serviceRequestsDF
    .where(col("complaint_type").contains("Noise"))
    .groupBy(col("incident_zip"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)


  // Question 8
  val drugActivityComplaintsByBoroughDF = serviceRequestsDF
    .where(col("complaint_type") === "Drug Activity")
    .groupBy(col("borough"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  val drugActivityComplaintsByZipcodeDF = serviceRequestsDF
    .where(col("complaint_type") === "Drug Activity")
    .groupBy(col("incident_zip"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 9
  val serviceRequestsByHourDF = serviceRequestsDF
    .groupBy(hour(col("created_date")).as("hour_of_the_day"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 10
  val avgResolutionTimeByAgencyDF = serviceRequestsDF
    .where(col("status") === "Closed")
    .groupBy(col("agency"))
    .agg(
      avg((col("closed_date").cast("long") - col("created_date").cast("long"))/86400)
        .as("avg_resolution_time_in_days")
    )
    .orderBy(col("avg_resolution_time_in_days").desc_nulls_last)


}
