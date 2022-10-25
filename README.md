# bigdataprogram-applaudo

## Apache Spark Big Data Project
This is a practice project to analyze a dataset through Apache Spark, creating a EDA report and finding useful insights to help create strategies or understand behaviours in the data.

## 1. About the Dataset
The dataset used, with title "311 Service Requests from 2010 to Present", was downloaded through:  https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9 

It contains 31,209,470 records, and each record represents the information about a service request made to the NYC 311. The NYC 311 line gives access to non-emergency City services and info about City government programs. The dataset has 41 columns, and the most characteristic ones are the following:

|    Column     | Description  |
|:---:|---|
|  Created Date   | Date SR was created |
|  Closed Date   | Date SR was closed by responding agency |
|  Agency   | Acronym of responding City Government Agency |
|  Agency Name  | Full Agency name of responding City Government Agency |
| Complaint Type | This is the first level of a hierarchy identifying the topic of the incident or condition. Complaint Type may have a corresponding Descriptor (below) or may stand alone. |
|     Descriptor     | This is associated to the Complaint Type, and provides further detail on the incident or condition. Descriptor values are dependent on the Complaint Type, and are not always required in SR. |
|     Incident Zip     | Incident location zip code, provided by geo validation. |
|     City     | City of the incident location provided by geovalidation. |
|   Status    | Status of SR submitted |
| Resolution Description  | Describes the last action taken on the SR by the responding agency. May describe next or future steps. |
| Resolution Action Updated Date  | Date when responding agency last updated the SR |
| Borough  | Provided by the submitter and confirmed by geovalidation. |
| Open Data Channel Type | Indicates how the SR was submitted to 311. i.e. By Phone, Online, Mobile, Other or Unknown. |


## 2. Exploratory Data Analysis (EDA) Report

### 2.1. Formatting
To improve the readability of the column names, the format was changed by removing the whitespaces and replacing the uppercase letters to lowercase. For example, "Created Date" was replaced by "created_date". These changes were introduced in the schema definition for the DataFrame: 

```scala
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
```
After specifying a schema for the DataFrame, it was required to read the dataset, that was in CSV format initially, but to improve the performance of the application it is preferred to have the dataset in Parquet format, so this is another transformation that will be performed to the dataset.

### 2.2. Missing data (Null Values)
After making an inspection of the columns in the dataset, it was possible to find the following percentages of missing values:

```scala
+------------------------------+----------+----------+
|column_name                   |null_count|percentage|
+------------------------------+----------+----------+
|vehicle_type                  |31198792  |99.97 %   |
|taxi_company_borough          |31185541  |99.92 %   |
|road_ramp                     |31139214  |99.77 %   |
|bridge_highway_direction      |31128080  |99.74 %   |
|bridge_highway_name           |31103545  |99.66 %   |
|bridge_highway_segment        |31099839  |99.65 %   |
|taxi_pick_up_location         |30976913  |99.25 %   |
|landmark                      |25986916  |83.27 %   |
|due_date                      |22528054  |72.18 %   |
|intersection_street_2         |21971039  |70.40 %   |
|intersection_street_1         |21965899  |70.38 %   |
|cross_street_2                |9867736   |31.62 %   |
|cross_street_1                |9745527   |31.23 %   |
|facility_type                 |9496370   |30.43 %   |
|bbl                           |7019934   |22.49 %   |
|location_type                 |6675010   |21.39 %   |
|street_name                   |4923127   |15.77 %   |
|incident_address              |4922571   |15.77 %   |
|address_type                  |4263402   |13.66 %   |
|longitude                     |2525247   |8.09 %    |
|latitude                      |2525247   |8.09 %    |
|x_coordinate                  |2525014   |8.09 %    |
|y_coordinate                  |2524803   |8.09 %    |
|location                      |2524731   |8.09 %    |
|city                          |1857054   |5.95 %    |
|incident_zip                  |1479586   |4.74 %    |
|resolution_description        |1226977   |3.93 %    |
|closed_date                   |881830    |2.83 %    |
|resolution_action_updated_date|456783    |1.46 %    |
|descriptor                    |96596     |0.31 %    |
|borough                       |47291     |0.15 %    |
|park_borough                  |47291     |0.15 %    |
|community_board               |47291     |0.15 %    |
|open_data_channel_type        |21        |0.00 %    |
|park_facility_name            |21        |0.00 %    |
|unique_key                    |0         |0.00 %    |
|created_date                  |0         |0.00 %    |
|agency                        |0         |0.00 %    |
|complaint_type                |0         |0.00 %    |
|agency_name                   |0         |0.00 %    |
|status                        |0         |0.00 %    |
+------------------------------+----------+----------+
```

Out of 41 columns, 16 of them have less than 5% of null values, so these are the columns that are going to be used for the main insights. The rest of the columns, specifically the ones with the highest percentage of missing data, correspond to columns that give information about certain types of complaints, some of them related to vehicles or taxis (for example columns like vehicle_type with 99.97% nulls and taxi_company_borough with 99.92% nulls), and about specific details of the service request's address.

The columns with more than 10% of missing values were removed, as these columns are not useful for the future insights in this project. The remaining columns of type string had the null values replaced by "Unspecified".

### 2.3. Outliers and inaccurate data

After inspecting the columns, it was possible to find that in the column "open_data_channel_type", there were 516 rows (0.165%) with channels of service request different to: "PHONE", "ONLINE", "MOBILE", "UNKNOWN" or "OTHER". As these are the channel types known and specified by the data source, the rows with other channels correspond to inaccurate data, so they were removed before obtaining the reports and insights.

## 3. Insights, Results and Analysis

In process...

### 3.1. What agencies have the highest number of service requests? 

```scala
+--------------------------------------------------+----------------------+----------+
|agency_name                                       |count_service_requests|percentage|
+--------------------------------------------------+----------------------+----------+
|New York City Police Department                   |8943589               |28.66 %   |
|Department of Housing Preservation and Development|7421521               |23.78 %   |
|Department of Transportation                      |3680622               |11.79 %   |
|Department of Sanitation                          |3644155               |11.68 %   |
|Department of Environmental Protection            |2175650               |6.97 %    |
|Department of Buildings                           |1419574               |4.55 %    |
|Department of Parks and Recreation                |1320413               |4.23 %    |
|Department of Health and Mental Hygiene           |766883                |2.46 %    |
|Taxi and Limousine Commission                     |314378                |1.01 %    |
|Department of Consumer Affairs                    |268304                |0.86 %    |
+--------------------------------------------------+----------------------+----------+
```

The report shows the 10 agencies with the most service requests made to the NYC 311, and the percentage of this count from the total amount of service requests. This report is useful to identify the agencies which need special attention or a bigger channel of communication and response for the public. 

**Code**

```scala
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
```
  
### 3.2. What are the most recurrent types of complaints in the agencies with the most service requests?

```scala
+--------------------------------------------------+-----------------------------------+------------------+----------------------------+--------------------+
|agency_name                                       |complaint_type                     |count_SR_by_agency|count_SR_by_agency/complaint|percentage_in_agency|
+--------------------------------------------------+-----------------------------------+------------------+----------------------------+--------------------+
|New York City Police Department                   |Noise - Residential                |8943589           |2889911                     |32.31 %             |
|New York City Police Department                   |Illegal Parking                    |8943589           |1731749                     |19.36 %             |
|New York City Police Department                   |Blocked Driveway                   |8943589           |1306452                     |14.61 %             |
|Department of Housing Preservation and Development|HEAT/HOT WATER                     |7421521           |1757917                     |23.69 %             |
|Department of Housing Preservation and Development|HEATING                            |7421521           |887720                      |11.96 %             |
|Department of Housing Preservation and Development|PLUMBING                           |7421521           |840512                      |11.33 %             |
|Department of Transportation                      |Street Condition                   |3680622           |1155611                     |31.40 %             |
|Department of Transportation                      |Street Light Condition             |3680622           |1080133                     |29.35 %             |
|Department of Transportation                      |Traffic Signal Condition           |3680622           |533054                      |14.48 %             |
|Department of Sanitation                          |Request Large Bulky Item Collection|3644155           |1073753                     |29.47 %             |
|Department of Sanitation                          |Dirty Conditions                   |3644155           |417008                      |11.44 %             |
|Department of Sanitation                          |Sanitation Condition               |3644155           |383019                      |10.51 %             |
|Department of Environmental Protection            |Water System                       |2175650           |806000                      |37.05 %             |
|Department of Environmental Protection            |Noise                              |2175650           |604089                      |27.77 %             |
|Department of Environmental Protection            |Sewer                              |2175650           |445863                      |20.49 %             |
+--------------------------------------------------+-----------------------------------+------------------+----------------------------+--------------------+
```

The report displays the complaint types with the highest amounts of service requests (SR) by agency, with the count of SR by agency, by complaint and agency, and the percentage of the service requests of that specific complaint from the agency total amount. This information can be used to identify the issues that need to be improved in each agency, and what strategies can be adopted to prevent certain complaints. 

**Code**

```scala
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
```

### 3.3. Which boroughs have the highest number of service requests? 

```scala
+-------------+----------------------+----------+
|      borough|count_service_requests|percentage|
+-------------+----------------------+----------+
|     BROOKLYN|               9270709|   29.70 %|
|       QUEENS|               7264167|   23.28 %|
|    MANHATTAN|               6046702|   19.37 %|
|        BRONX|               5769470|   18.49 %|
|STATEN ISLAND|               1573200|    5.04 %|
|  Unspecified|               1284685|    4.12 %|
+-------------+----------------------+----------+

```  

The report gives information about the amount of SR per borough, with the percentage that this number represents to the total amount of SR. The insight can be applied to identify geographically the locations in New York where agencies should pay more attention to reduce incidences.

**Code**

```scala
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
```

### 3.4. What are the most common channels through which service requests are made? 

```scala
+----------------------+----------------------+----------+
|open_data_channel_type|count_service_requests|percentage|
+----------------------+----------------------+----------+
|                 PHONE|              14717191|   47.16 %|
|                ONLINE|               6846945|   21.94 %|
|               UNKNOWN|               6238820|   19.99 %|
|                MOBILE|               3037970|    9.73 %|
|                 OTHER|                368007|    1.18 %|
+----------------------+----------------------+----------+

``` 

The report presents the different types of channels through which the service requests are made, and their total number of SR. This helps to know which channels are the ones that the citizens use the most, and how can the NYC 311 improve the communication with the public and maintain the channels working properly.

**Code**

```scala
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
```

### 3.5. How has the channel evolved in the last years? 

```scala
+----+----------------------+----------------+------------------------+----------+
|year|open_data_channel_type|count_SR_by_year|count_SR_by_year/channel|percentage|
+----+----------------------+----------------+------------------------+----------+
|2010|               UNKNOWN|         2089568|                 1159221|   55.48 %|
|2010|                 PHONE|         2089568|                  824477|   39.46 %|
|2010|                 OTHER|         2089568|                   64712|    3.10 %|
|2010|                ONLINE|         2089568|                   41158|    1.97 %|
|2011|               UNKNOWN|         2011028|                 1093240|   54.36 %|
|2011|                 PHONE|         2011028|                  762385|   37.91 %|
|2011|                ONLINE|         2011028|                   96427|    4.79 %|
|2011|                 OTHER|         2011028|                   58974|    2.93 %|
|2011|                MOBILE|         2011028|                       2|    0.00 %|
|2012|                 PHONE|         1837049|                  970540|   52.83 %|
|2012|               UNKNOWN|         1837049|                  658090|   35.82 %|
|2012|                ONLINE|         1837049|                  166379|    9.06 %|
|2012|                 OTHER|         1837049|                   42040|    2.29 %|
|2013|                 PHONE|         1887471|                 1258535|   66.68 %|
|2013|               UNKNOWN|         1887471|                  317052|   16.80 %|
|2013|                ONLINE|         1887471|                  259160|   13.73 %|
|2013|                 OTHER|         1887471|                   43441|    2.30 %|
|2013|                MOBILE|         1887471|                    9283|    0.49 %|
|2014|                 PHONE|         2156725|                 1306082|   60.56 %|
|2014|                ONLINE|         2156725|                  386264|   17.91 %|
|2014|               UNKNOWN|         2156725|                  360149|   16.70 %|
|2014|                MOBILE|         2156725|                   70271|    3.26 %|
|2014|                 OTHER|         2156725|                   33959|    1.57 %|
|2015|                 PHONE|         2322422|                 1314670|   56.61 %|
|2015|                ONLINE|         2322422|                  463875|   19.97 %|
|2015|               UNKNOWN|         2322422|                  367315|   15.82 %|
|2015|                MOBILE|         2322422|                  151617|    6.53 %|
|2015|                 OTHER|         2322422|                   24945|    1.07 %|
|2016|                 PHONE|         2408922|                 1283903|   53.30 %|
|2016|                ONLINE|         2408922|                  512282|   21.27 %|
|2016|               UNKNOWN|         2408922|                  337263|   14.00 %|
|2016|                MOBILE|         2408922|                  246970|   10.25 %|
|2016|                 OTHER|         2408922|                   28504|    1.18 %|
|2017|                 PHONE|         2508407|                 1333598|   53.17 %|
|2017|                ONLINE|         2508407|                  521442|   20.79 %|
|2017|               UNKNOWN|         2508407|                  346906|   13.83 %|
|2017|                MOBILE|         2508407|                  277130|   11.05 %|
|2017|                 OTHER|         2508407|                   29331|    1.17 %|
|2018|                 PHONE|         2760061|                 1480708|   53.65 %|
|2018|                ONLINE|         2760061|                  565393|   20.48 %|
|2018|               UNKNOWN|         2760061|                  367205|   13.30 %|
|2018|                MOBILE|         2760061|                  314251|   11.39 %|
|2018|                 OTHER|         2760061|                   32504|    1.18 %|
|2019|                 PHONE|         2633099|                 1309555|   49.73 %|
|2019|                ONLINE|         2633099|                  549347|   20.86 %|
|2019|               UNKNOWN|         2633099|                  401574|   15.25 %|
|2019|                MOBILE|         2633099|                  364054|   13.83 %|
|2019|                 OTHER|         2633099|                    8569|    0.33 %|
|2020|                 PHONE|         2942050|                 1152892|   39.19 %|
|2020|                ONLINE|         2942050|                  904251|   30.74 %|
|2020|                MOBILE|         2942050|                  535269|   18.19 %|
|2020|               UNKNOWN|         2942050|                  349028|   11.86 %|
|2020|                 OTHER|         2942050|                     610|    0.02 %|
|2021|                ONLINE|         3220872|                 1294503|   40.19 %|
|2021|                 PHONE|         3220872|                 1124751|   34.92 %|
|2021|                MOBILE|         3220872|                  544609|   16.91 %|
|2021|               UNKNOWN|         3220872|                  256638|    7.97 %|
|2021|                 OTHER|         3220872|                     371|    0.01 %|
|2022|                ONLINE|         2431259|                 1086464|   44.69 %|
|2022|                 PHONE|         2431259|                  595095|   24.48 %|
|2022|                MOBILE|         2431259|                  524514|   21.57 %|
|2022|               UNKNOWN|         2431259|                  225139|    9.26 %|
|2022|                 OTHER|         2431259|                      47|    0.00 %|
+----+----------------------+----------------+------------------------+----------+

```

The report displays the evolution of the number of SR by channel type over the years. By knowing how the number of SR by channel type increases over the years, it is possible to predict which channels will be preferred by the public in the future, and thus increase channel capacity.

**Code**

```scala
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
```

### 3.6. What are the boroughs and zipcodes that have the highest number of the most recurrent complaint types?
  
```scala
+-------------------+-------------+------------+---------------------+-----------------------------+-----------------------+
|complaint_type     |borough      |incident_zip|count_SR_by_complaint|count_SR_by_complaint/zipcode|percentage_in_complaint|
+-------------------+-------------+------------+---------------------+-----------------------------+-----------------------+
|Noise - Residential|BRONX        |10466       |2886923              |224547                       |7.78 %                 |
|Noise - Residential|BRONX        |10467       |2886923              |59960                        |2.08 %                 |
|Noise - Residential|BROOKLYN     |11226       |2886923              |58539                        |2.03 %                 |
|HEAT/HOT WATER     |BROOKLYN     |11226       |1750911              |58545                        |3.34 %                 |
|HEAT/HOT WATER     |BRONX        |10458       |1750911              |53976                        |3.08 %                 |
|HEAT/HOT WATER     |BRONX        |10467       |1750911              |52422                        |2.99 %                 |
|Illegal Parking    |QUEENS       |11385       |1728819              |49164                        |2.84 %                 |
|Illegal Parking    |BROOKLYN     |11385       |1728819              |49164                        |2.84 %                 |
|Illegal Parking    |BROOKLYN     |11204       |1728819              |29969                        |1.73 %                 |
|Illegal Parking    |BROOKLYN     |11214       |1728819              |28932                        |1.67 %                 |
|Blocked Driveway   |QUEENS       |11368       |1304735              |42828                        |3.28 %                 |
|Blocked Driveway   |QUEENS       |11385       |1304735              |31525                        |2.42 %                 |
|Blocked Driveway   |BROOKLYN     |11385       |1304735              |31525                        |2.42 %                 |
|Blocked Driveway   |BROOKLYN     |11236       |1304735              |27059                        |2.07 %                 |
|Street Condition   |STATEN ISLAND|10314       |1097396              |24224                        |2.21 %                 |
|Street Condition   |STATEN ISLAND|10306       |1097396              |20138                        |1.84 %                 |
|Street Condition   |STATEN ISLAND|10312       |1097396              |19529                        |1.78 %                 |
+-------------------+-------------+------------+---------------------+-----------------------------+-----------------------+

```

The report shows the most recurrent complaint types, and the boroughs and zipcodes where they happen the most. This table is useful to focus the strategies in certain locations to prevent the incidents that citizens report most frequently.

**Code**

```scala
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
```


### 3.7. What are the hours of the day with the most service requests and the most recurrent complaint type in those hours? 


```scala
+----+------------------------+----------------+--------------------------+---------------+
|hour|recurrent_complaint_type|count_SR_by_hour|count_SR_by_hour/complaint|percentage_hour|
+----+------------------------+----------------+--------------------------+---------------+
|0   |HEATING                 |4535532         |887836                    |14.53 %        |
|12  |Derelict Vehicles       |2022136         |323132                    |6.48 %         |
|11  |Street Light Condition  |1896533         |171674                    |6.08 %         |
|10  |Street Light Condition  |1892084         |172372                    |6.06 %         |
|9   |Street Light Condition  |1742452         |116162                    |5.58 %         |
|14  |Street Condition        |1729392         |120752                    |5.54 %         |
|13  |Street Condition        |1702601         |91412                     |5.46 %         |
|15  |Street Condition        |1621149         |93369                     |5.19 %         |
|16  |Noise - Residential     |1485974         |82764                     |4.76 %         |
|8   |Illegal Parking         |1279304         |106613                    |4.10 %         |
|17  |Noise - Residential     |1257331         |96098                     |4.03 %         |
|22  |Noise - Residential     |1243115         |321054                    |3.98 %         |
|18  |Noise - Residential     |1197350         |116593                    |3.84 %         |
|21  |Noise - Residential     |1185247         |231788                    |3.80 %         |
|23  |Noise - Residential     |1139298         |351562                    |3.65 %         |
|19  |Noise - Residential     |1135445         |143628                    |3.64 %         |
|20  |Noise - Residential     |1123556         |180108                    |3.60 %         |
|7   |Illegal Parking         |777515          |88217                     |2.49 %         |
|1   |Noise - Residential     |623982          |220045                    |2.00 %         |
|6   |Illegal Parking         |446899          |50576                     |1.43 %         |
|2   |Noise - Residential     |414182          |140980                    |1.33 %         |
|3   |Noise - Residential     |280216          |92042                     |0.90 %         |
|5   |Noise - Residential     |247326          |43442                     |0.79 %         |
|4   |Noise - Residential     |230314          |61690                     |0.74 %         |
+----+------------------------+----------------+--------------------------+---------------+

```   

The report is ordered from the hours with the highest number of service requests and the most recurrent complaint type in that hour. There is an increase in the number of service requests around midnight and in the morning, so the responding agencies and the NYC 311 have to make sure that there are enough agents to answer the requests in the most busy hours.

**Code**

```scala
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
```

### 3.8. What is the average resolution time by agency? ###
  
```scala
+---------------------------------------+---------------------------+
|agency                                 |avg_resolution_time_in_days|
+---------------------------------------+---------------------------+
|FDNY                                   |402.1443981481481          |
|DPR                                    |91.80619877633934          |
|DOB                                    |79.55560086618235          |
|TLC                                    |72.34416505093907          |
|EDC                                    |61.99349087209839          |
|DOE                                    |58.7220819186892           |
|DOITT                                  |28.759503091629163         |
|OFFICE OF TECHNOLOGY AND INNOVATION    |22.499034288194444         |
|DCA                                    |21.78509000448973          |
|DOHMH                                  |20.072018912219            |
|DOT                                    |13.75357668500991          |
|NYCEM                                  |13.64512206574749          |
|HPD                                    |13.295751961811682         |
|MAYORâS OFFICE OF SPECIAL ENFORCEMENT|10.272963081209197         |
|DSNY                                   |9.240251102860649          |
|DEP                                    |8.211337005506733          |
|DOF                                    |7.375863857895153          |
|DFTA                                   |5.7700290651236985         |
|DORIS                                  |3.549078047263682          |
|DHS                                    |1.3539195857965487         |
|ACS                                    |0.9500694444444444         |
|NYPD                                   |0.2864826985184468         |
|OSE                                    |0.12648533950617283        |
|3-1-1                                  |0.010439375414083142       |
|HRA                                    |6.281035374883267E-4       |
+---------------------------------------+---------------------------+

```   
  
  
