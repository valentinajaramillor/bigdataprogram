import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object exploration extends App {

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

  // Reading the dataset from the parquet file to a DataFrame
  val serviceRequestsDF = spark.read
    .schema(serviceRequestsSchema)
    .option("timestampFormat", "MM/dd/YYYY hh:mm:ss aa")
    .parquet("src/main/resources/data/311_Service_Requests_from_2010_to_Present")

  // Creating a new format for the percentages
  val percentageFormat: Column => Column = (number:Column) => concat(format_number(number*100,2),lit(" %"))

  // Counting the total amount of rows in the dataframe
  val totalRows = serviceRequestsDF.count()

  // Inspecting missing values
  def countNullsCols(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      count(when(col(c).isNull, c)).alias(c)
    })
  }

  val cols = Seq("unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip", "incident_address",
    "street_name", "cross_street_1", "cross_street_2" ,"intersection_street_1", "intersection_street_2",
    "address_type", "city", "landmark", "facility_type" , "status", "due_date", "resolution_description",
    "resolution_action_updated_date", "community_board", "bbl", "borough", "x_coordinate", "y_coordinate",
    "open_data_channel_type","park_facility_name","park_borough","vehicle_type","taxi_company_borough",
    "taxi_pick_up_location","bridge_highway_name","bridge_highway_direction","road_ramp",
    "bridge_highway_segment","latitude","longitude","location")

  import org.apache.spark.sql.{functions => func, _}

  // Creating a DataFrame with the column names and statistics of null values
  val nullStatsDF = serviceRequestsDF.select(
    countNullsCols(serviceRequestsDF.columns): _*)
    .select(
      func.explode(
        func.array(
          cols.map(
            col =>
              func.struct(
                func.lit(col).alias("column_name"),
                func.col(col).alias("null_count")
              )
          ): _*)
      ).alias("v")
    )
    .selectExpr("v.*")
    .withColumn("percentage", percentageFormat(col("null_count") / totalRows))
    .orderBy(col("null_count").desc)

  // The following code is to find the columns that have more than 10% of null values
  import spark.implicits._

  val nullColumnsDF = nullStatsDF
    .where(col("null_count") > 3100000)
    .orderBy(col("null_count").desc)
    //.map(f => f.getString(0))
    //.collect()
    //.toList

  // Creating a list of the previous columns
  val listOfNullColumns = nullStatsDF
    .where(col("null_count") > 3100000)
    .map(f => f.getString(0))
    .collect()
    .toList

  //nullColumnsDF.show()
  //println(listOfNullColumns)

  // Creating the DataFrame of the columns with less than 10% of null values
  val remainingColumnsList = serviceRequestsDF.columns.diff(listOfNullColumns)
  val shorterServiceRequestsDF = serviceRequestsDF.select(
    remainingColumnsList.map(
      cn => col(cn)
    ): _*
  )


  // Replacing the remaining null values with undefined
  val withUnspecifiedDF = shorterServiceRequestsDF.na
    .fill("Unspecified")


  // Find outliers and cleaning representative columns

  //Outliers in open_data_channel_type
  val countChannelOutliers = serviceRequestsDF
    .where(
      col("open_data_channel_type") =!= "PHONE" and
        col("open_data_channel_type") =!= "ONLINE" and
        col("open_data_channel_type") =!= "MOBILE" and
        col("open_data_channel_type") =!= "UNKNOWN" and
        col("open_data_channel_type") =!= "OTHER")
    .count()

  //println(countChannelOutliers)
  //println((countChannelOutliers.toDouble/totalRows.toDouble)*100)

  //Remove outliers in open_data_channel_type
  val cleanedServiceRequestsDF = withUnspecifiedDF
    .where(
      col("open_data_channel_type") === "PHONE" or
        col("open_data_channel_type") === "ONLINE" or
        col("open_data_channel_type") === "MOBILE" or
        col("open_data_channel_type") === "UNKNOWN" or
        col("open_data_channel_type") === "OTHER")


  // Initial Insights

  /**
   * Questions:
   *
   * 1. Which agencies have the highest number of service requests?
   * 2. What are the most recurrent types of complaints in the agencies with the most service requests?
   * 3. Which boroughs have the highest number of service requests?
   * 4. What are the most common channels through which service requests are made?
   * 5. How has the channel evolved in the last years?
   * 6. Which are the boroughs and zipcodes that have the highest number of noise complaints?
   * 7. Which are the boroughs and zipcodes that have the highest number of drug activity complaints?
   * 8. What is the hour of the day with the most service requests?
   * 9. What is the average resolution time by agency?
   */

  import org.apache.spark.sql.functions._

  // Question 1
  // Find the number of service requests by agency, and get the percentage of this amount from the total number of rows
  val serviceRequestsByAgencyDF = cleanedServiceRequestsDF
    .groupBy(col("agency_name"))
    .agg(
      count("*").as("count_service_requests")
    )
    .withColumn(
      "percentage", percentageFormat(col("count_service_requests") / totalRows)
    )
    .orderBy(col("count_service_requests").desc_nulls_last)
    .limit(10)


  // Question 2

  //def serviceRequestsByAgencyByComplaintDF(agencyName: String) = cleanedServiceRequestsDF
  //  .where(col("agency_name") === agencyName)
  //  .groupBy(col("complaint_type"))
  //  .agg(count("*").as("count"))
  //  .orderBy(col("count").desc_nulls_last)

  //val serviceRequestsPoliceByComplaintDF = serviceRequestsByAgencyByComplaintDF("New York City Police Department")
  //val serviceRequestsHousingByComplaintDF = serviceRequestsByAgencyByComplaintDF("Department of Housing Preservation and Development")
  //val serviceRequestsTransportationByComplaintDF = serviceRequestsByAgencyByComplaintDF("Department of Transportation")

  // Create the Window functions partitioned by agency and by agency and complaint type
  val wAgency = Window.partitionBy("agency_name")
  val wAgencyAndComplaint = Window.partitionBy("agency_name", "complaint_type")

  // Count service requests by agency, by agency and complaint, and create ranks based on that
  val mostRecurrentComplaintsByAgencyDF = cleanedServiceRequestsDF
    .withColumn(
      "count_SR_by_agency",
      count(lit(1)).over(wAgency)
    )
    .withColumn(
      "count_SR_by_agency/complaint",
      count(lit(1)).over(wAgencyAndComplaint)
    )
    .withColumn(
      "rank_agency",
      dense_rank().over(Window.orderBy(col("count_SR_by_agency").desc))
    )
    .withColumn(
      "rank_complaint",
      dense_rank().over(wAgency.orderBy(col("count_SR_by_agency/complaint").desc))
    )
    .withColumn(
      "percentage_in_agency", percentageFormat(col("count_SR_by_agency/complaint") / col("count_SR_by_agency"))
    )
    // Filter by the agencies and complaints with more service requests
    .where(
      (col("rank_complaint") <= 3) and
        (col("rank_agency") <= 5)
    )
    // Select the required columns for the report
    .select(
      col("agency_name"),
      col("complaint_type"),
      col("count_SR_by_agency"),
      col("count_SR_by_agency/complaint"),
      col("percentage_in_agency")
    )
    .distinct()
    .orderBy(col("count_SR_by_agency").desc, col("count_SR_by_agency/complaint").desc)


  // Question 3
  // Find the boroughs with the highest amount of service requests
  // Group by borough, count rows, add percentage of count from total rows and order by the count
  val serviceRequestsByBoroughDF = cleanedServiceRequestsDF
    .groupBy(col("borough"))
    .agg(
      count("*").as("count_service_requests")
    )
    .withColumn(
      "percentage", percentageFormat(col("count_service_requests") / totalRows)
    )
    .orderBy(col("count_service_requests").desc)


  // Question 4
  // Find the open data channel types with the highest amount of service requests
  // Group by open data channel type, count rows, add percentage of count from total rows and order by the count
  val serviceRequestsByChannelDF = cleanedServiceRequestsDF
    .groupBy(col("open_data_channel_type"))
    .agg(
      count("*").as("count_service_requests")
    )
    .withColumn(
      "percentage", percentageFormat(col("count_service_requests") / totalRows)
    )
    .orderBy(col("count_service_requests").desc)

  serviceRequestsByChannelDF.show()

  // Question 5
  // Create the window functions partitioned by year and by year and channel
  val wYear = Window.partitionBy(col("year"))
  val wYearAndChannel = Window.partitionBy(col("year"), col("open_data_channel_type"))

  // Create column with creation year, count of service requests SR by year and by year and channel
  // and the percentage of the count of SR from the year's total count
  val channelEvolutionDF = cleanedServiceRequestsDF
    .select(
      col("created_date"), col("open_data_channel_type")
    )
    .withColumn("year",
      year(col("created_date"))
    )
    .withColumn("count_SR_by_year",
      count(lit(1)).over(wYear)
    )
    .withColumn("count_SR_by_year/channel",
      count(lit(1)).over(wYearAndChannel)
    )
    .withColumn("percentage",
      percentageFormat(col("count_SR_by_year/channel") / col("count_SR_by_year"))
    )
    .select(
      col("year"),
      col("open_data_channel_type"),
      col("count_SR_by_year"),
      col("count_SR_by_year/channel"),
      col("percentage")
    )
    .distinct()
    .orderBy(col("year"), col("count_SR_by_year/channel").desc)


  // Question 6
  val noiseComplaintsByBoroughDF = cleanedServiceRequestsDF
    .where(col("complaint_type").contains("Noise"))
    .groupBy(col("borough"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  val noiseComplaintsByZipcodeDF = serviceRequestsDF
    .where(col("complaint_type").contains("Noise"))
    .groupBy(col("incident_zip"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)


  // Question 7
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

  // Question 8
  val serviceRequestsByHourDF = serviceRequestsDF
    .groupBy(hour(col("created_date")).as("hour_of_the_day"))
    .agg(count("*").as("count"))
    .orderBy(col("count").desc_nulls_last)

  // Question 9
  val avgResolutionTimeByAgencyDF = serviceRequestsDF
    .where(col("status") === "Closed")
    .groupBy(col("agency"))
    .agg(
      avg((col("closed_date").cast("long") - col("created_date").cast("long"))/86400)
        .as("avg_resolution_time_in_days")
    )
    .orderBy(col("avg_resolution_time_in_days").desc_nulls_last)


}
