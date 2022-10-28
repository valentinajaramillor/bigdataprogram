import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, concat, count, dense_rank, format_number, hour, lit, max, min, month, round, stddev, when, year}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}


object exploration extends App {

  val spark = SparkSession.builder()
    .appName("exploration")
    .master("local[*]")
    .config("spark.driver.memory","7g")
    .config("spark.sql.shuffle.partitions", "5")
    .config("spark.memory.offHeap.enabled", true)
    .config("spark.memory.offHeap.size", "7g")
    .getOrCreate()

  // Configuration to use the legacy format in newer versions of Spark
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

  import org.apache.spark.sql.{functions => func}

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


  // cleanedServiceRequestsDF.write
  //   .mode(SaveMode.Overwrite)
  //   .parquet("src/main/resources/data/311_Service_Requests_from_2010_to_Present_Cleaned")



  /**
   * Questions:
   *
   * 1. Which agencies have the highest number of service requests?
   * 2. What are the most recurrent types of complaints in the agencies with the most service requests?
   * 3. Which boroughs have the highest number of service requests?
   * 4. What are the most common channels through which service requests are made?
   * 5. How has the channel evolved in the last years?
   * 6. What are the boroughs and zipcodes that have the highest number of the most recurrent complaint types?
   * 7. What are the hours of the day with the most service requests and the most recurrent complaint type in those hours?
   * 8. What is the average resolution time by agency?
   * 9. What are the most recurrent complaint types by month of the year?
   */


  val sampleDF = cleanedServiceRequestsDF.sample(false, 0.10)

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
  // Find the most recurrent complaint types and the zipcodes where they happen the most

  // Create the Window functions partitioned by complaint type and by complaint type and incident zip
  val wComplaint = Window.partitionBy("complaint_type")
  val wComplaintAndZipcode = Window.partitionBy("complaint_type", "incident_zip")

  // Filter to exclude the unspecified zips and borough
  // Count service requests by complaint type and by complaint type and incident zip, and create ranks based on that
  val zipcodesByComplaintDF = cleanedServiceRequestsDF
    .where(col("incident_zip") =!= "Unspecified" and
      col("borough") =!= "Unspecified")
    .withColumn(
      "count_SR_by_complaint",
      count(lit(1)).over(wComplaint)
    )
    .withColumn(
      "count_SR_by_complaint/zipcode",
      count(lit(1)).over(wComplaintAndZipcode)
    )
    .withColumn(
      "rank_complaint",
      dense_rank().over(Window.orderBy(col("count_SR_by_complaint").desc))
    )
    .withColumn(
      "rank_zipcode",
      dense_rank().over(wComplaint.orderBy(col("count_SR_by_complaint/zipcode").desc))
    )
    // Filter by the zipcodes and complaints with more service requests
    .where(
      (col("rank_zipcode") <= 3) and
        (col("rank_complaint") <= 5)
    )
    .withColumn(
      "percentage_in_complaint", percentageFormat(col("count_SR_by_complaint/zipcode") / col("count_SR_by_complaint"))
    )
    // Select the required columns for the report
    .select(
      col("complaint_type"),
      col("borough"),
      col("incident_zip"),
      col("count_SR_by_complaint"),
      col("count_SR_by_complaint/zipcode"),
      col("percentage_in_complaint")
    )
    .distinct()
    .orderBy(col("count_SR_by_complaint").desc, col("count_SR_by_complaint/zipcode").desc)


  // Question 7

  // Create the Window functions partitioned by hour and by hour and complaint type
  val wHour = Window.partitionBy("hour")
  val wHourAndComplaint = Window.partitionBy("hour", "complaint_type")

  // Filter to exclude the unspecified complaint types
  // Count service requests by hour and by hour and complaint type, and create ranks based on that
  val serviceRequestsByHourDF = cleanedServiceRequestsDF
    .where(col("complaint_type") =!= "Unspecified")
    .withColumn(
      "hour",
      hour(col("created_date"))
    )
    .withColumn(
      "count_SR_by_hour",
      count(lit(1)).over(wHour)
    )
    .withColumn(
      "count_SR_by_hour/complaint",
      count(lit(1)).over(wHourAndComplaint)
    )
    .withColumn(
      "rank_complaint",
      dense_rank().over(wHour.orderBy(col("count_SR_by_hour/complaint").desc))
    )
    // Filter by the most recurrent complaint type in each hour
    .where(col("rank_complaint") === 1)
    .withColumn(
      "percentage_hour", percentageFormat(col("count_SR_by_hour") / totalRows)
    )
    // Select the required columns for the report
    .select(
      col("hour"),
      col("complaint_type").as("recurrent_complaint_type"),
      col("count_SR_by_hour"),
      col("count_SR_by_hour/complaint"),
      col("percentage_hour"),
    )
    .distinct()
    .orderBy(col("count_SR_by_hour").desc)


  // Question 8
  // Find the agencies with the highest average resolution times, and some variability measures
  // First, create the window function partitioned by agency
  // val wAgency = Window.partitionBy("agency_name")

  // Filter by the service requests that are closed, and measure the resolution time of each service request
  // Find the average, max, min, stddev and coefficient of variation
  val avgResolutionTimeByAgencyDF = cleanedServiceRequestsDF
    .where(col("status") === "Closed" and
    col("agency_name").contains("School") === false)
    .withColumn("resolution_time_days",
      round((col("closed_date").cast("long") -
        col("created_date").cast("long")) / 86400,4)
    )
    .withColumn("avg_time",
      round(avg(col("resolution_time_days")).over(wAgency),4)
    )
    .withColumn("max_time",
      round(max(col("resolution_time_days")).over(wAgency),4)
    )
    .withColumn("min_time",
      when(round(min(col("resolution_time_days")).over(wAgency),4) < 0, "Undefined")
        .otherwise(round(min(col("resolution_time_days")).over(wAgency),4))
    )
    .withColumn("stddev",
      round(stddev(col("resolution_time_days")).over(wAgency),4)
    )
    .withColumn("coefficient_var",
      round(col("stddev") / col("avg_time"),2)
    )
    // Select the required columns for the report
    .select(
      col("agency_name"),
      col("avg_time"),
      col("max_time"),
      col("min_time"),
      col("stddev"),
      col("coefficient_var")
    )
    .distinct()
    .na.fill("Undefined", Seq("avg_time","max_time","min_time","stddev","coefficient_var"))
    .limit(15)
    .orderBy(col("avg_time").desc)


  // Question 9
  // Find the most recurrent complaint types by month of the year

  // Create the Window functions partitioned by month and by month and complaint type
  val wMonth = Window.partitionBy("month")
  val wMonthAndComplaint = Window.partitionBy("month", "complaint_type")

  // Filter to exclude the unspecified complaint types
  // Count service requests by month and by month and complaint type, and create ranks based on that
  val complaintsByMonthDF = cleanedServiceRequestsDF
    .where(col("complaint_type") =!= "Unspecified")
    .withColumn(
      "month",
      month(col("created_date"))
    )
    .withColumn(
      "count_SR_by_month",
      count(lit(1)).over(wMonth)
    )
    .withColumn(
      "count_SR_by_month/complaint",
      count(lit(1)).over(wMonthAndComplaint)
    )
    .withColumn(
      "rank_complaint",
      dense_rank().over(wMonth.orderBy(col("count_SR_by_month/complaint").desc))
    )
    // Filter by the complaints with more service requests
    .where(
      col("rank_complaint") <= 3
    )
    .withColumn(
      "percentage_month", percentageFormat(col("count_SR_by_month") / totalRows)
    )
    .withColumn(
      "percentage_complaint/month", percentageFormat(col("count_SR_by_month/complaint") / col("count_SR_by_month"))
    )
    // Select the required columns for the report
    .select(
      col("month"),
      col("complaint_type").as("recurrent_complaint_types"),
      col("count_SR_by_month"),
      col("percentage_month"),
      col("count_SR_by_month/complaint"),
      col("percentage_complaint/month")
    )
    .distinct()
    .orderBy(col("month"), col("count_SR_by_month/complaint").desc)

}
