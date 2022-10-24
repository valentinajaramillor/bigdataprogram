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

***New York City Police Department  

```scala
+-----------------------+-------+
|complaint_type         |count  |
+-----------------------+-------+
|Noise - Residential    |2889911|
|Illegal Parking        |1731749|
|Blocked Driveway       |1306452|
|Noise - Street/Sidewalk|994535 |
|Noise - Commercial     |493605 |
+-----------------------+-------+

```
  
***Department of Housing Preservation and Development  


```scala
+--------------------+-------+
|complaint_type      |count  |
+--------------------+-------+
|HEAT/HOT WATER      |1757917|
|HEATING             |887720 |
|PLUMBING            |840512 |
|UNSANITARY CONDITION|675271 |
|GENERAL CONSTRUCTION|500821 |
+--------------------+-------+

``` 
  
***Department of Transportation  

```scala
+------------------------+-------+
|complaint_type          |count  |
+------------------------+-------+
|Street Condition        |1155611|
|Street Light Condition  |1080133|
|Traffic Signal Condition|533054 |
|Sidewalk Condition      |319892 |
|Broken Muni Meter       |172113 |
+------------------------+-------+

```
  
### 3.3. Which cities have the highest number of service requests? ###

```scala
+-------------+-------+
|city         |count  |
+-------------+-------+
|BROOKLYN     |9208752|
|NEW YORK     |5778849|
|BRONX        |5739577|
|null         |1857054|
|STATEN ISLAND|1547592|
+-------------+-------+

``` 

### 3.4. Which boroughs have the highest number of service requests? ###

```scala
+-------------+-------+
|borough      |count  |
+-------------+-------+
|BROOKLYN     |9270709|
|QUEENS       |7264167|
|MANHATTAN    |6046702|
|BRONX        |5769470|
|STATEN ISLAND|1573200|
+-------------+-------+

```  

### 3.5. What are the most common channels through which service requests are made? ###

```scala
+----------------------+--------+
|open_data_channel_type|count   |
+----------------------+--------+
|PHONE                 |14717191|
|ONLINE                |6846945 |
|UNKNOWN               |6238820 |
|MOBILE                |3037970 |
|OTHER                 |368007  |
|null                  |21      |
|990400                |5       |
|975580                |5       |
|993558                |5       |
|993461                |3       |
|1005067               |3       |
|995799                |3       |
|983424                |3       |
|990880                |3       |
|1001324               |3       |
|984598                |3       |
|1004745               |3       |
|1000346               |3       |
|990003                |2       |
|1017392               |2       |
+----------------------+--------+

``` 


### 3.6. How has the channel evolved in the last years? ###
  
```scala
+------------+----------------------+-------+
|created_year|open_data_channel_type|  count|
+------------+----------------------+-------+
|        2010|               UNKNOWN|1159221|
|        2010|                 PHONE| 824477|
|        2010|                 OTHER|  64712|
|        2010|                ONLINE|  41158|
|        2011|               UNKNOWN|1093240|
|        2011|                 PHONE| 762385|
|        2011|                ONLINE|  96427|
|        2011|                 OTHER|  58974|
|        2011|                MOBILE|      2|
|        2012|                 PHONE| 970540|
|        2012|               UNKNOWN| 658090|
|        2012|                ONLINE| 166379|
|        2012|                 OTHER|  42040|
|        2013|                 PHONE|1258535|
|        2013|               UNKNOWN| 317052|
|        2013|                ONLINE| 259160|
|        2013|                 OTHER|  43441|
|        2013|                MOBILE|   9283|
|        2014|                 PHONE|1306082|
|        2014|                ONLINE| 386264|
|        2014|               UNKNOWN| 360149|
|        2014|                MOBILE|  70271|
|        2014|                 OTHER|  33959|
|        2015|                 PHONE|1314670|
|        2015|                ONLINE| 463875|
|        2015|               UNKNOWN| 367315|
|        2015|                MOBILE| 151617|
|        2015|                 OTHER|  24945|
|        2016|                 PHONE|1283903|
|        2016|                ONLINE| 512282|
|        2016|               UNKNOWN| 337263|
|        2016|                MOBILE| 246970|
|        2016|                 OTHER|  28504|
|        2017|                 PHONE|1333598|
|        2017|                ONLINE| 521442|
|        2017|               UNKNOWN| 346906|
|        2017|                MOBILE| 277130|
|        2017|                 OTHER|  29331|
|        2018|                 PHONE|1480708|
|        2018|                ONLINE| 565393|
|        2018|               UNKNOWN| 367205|
|        2018|                MOBILE| 314251|
|        2018|                 OTHER|  32504|
|        2019|                 PHONE|1309555|
|        2019|                ONLINE| 549347|
|        2019|               UNKNOWN| 401574|
|        2019|                MOBILE| 364054|
|        2019|                 OTHER|   8569|
|        2020|                 PHONE|1152892|
|        2020|                ONLINE| 904251|
|        2020|                MOBILE| 535269|
|        2020|               UNKNOWN| 349028|
|        2020|                 OTHER|    610|
|        2021|                ONLINE|1294503|
|        2021|                 PHONE|1124751|
|        2021|                MOBILE| 544609|
|        2021|               UNKNOWN| 256638|
|        2021|                 OTHER|    371|
|        2022|                ONLINE|1086464|
|        2022|                 PHONE| 595095|
|        2022|                MOBILE| 524514|
|        2022|               UNKNOWN| 225139|
|        2022|                 OTHER|     47|
+------------+----------------------+-------+

``` 


### 3.7. Which are the boroughs and zipcodes that have the highest number of noise complaints? ###
  
```scala
+-------------+-------+
|borough      |count  |
+-------------+-------+
|MANHATTAN    |1624180|
|BROOKLYN     |1486861|
|BRONX        |1326379|
|QUEENS       |997733 |
|STATEN ISLAND|135684 |
+-------------+-------+

```

  
```scala
+------------+------+
|incident_zip|count |
+------------+------+
|10466       |237859|
|10031       |103588|
|11226       |97768 |
|10032       |92342 |
|10034       |88167 |
+------------+------+

```  

### 3.8. Which are the boroughs and zipcodes that have the highest number of drug activity complaints? ###
  
```scala
+-------------+-----+
|borough      |count|
+-------------+-----+
|MANHATTAN    |7096 |
|QUEENS       |6269 |
|BROOKLYN     |4311 |
|BRONX        |3706 |
|STATEN ISLAND|698  |
+-------------+-----+

```   

```scala
+------------+-----+
|incident_zip|count|
+------------+-----+
|10035       |2545 |
|11432       |1238 |
|10472       |672  |
|11213       |667  |
|11366       |614  |
+------------+-----+

```  


### 3.9. What is the hour of the day with the most service requests? ###

```scala
+---------------+-------+
|hour_of_the_day|  count|
+---------------+-------+
|              0|4535542|
|             12|2022173|
|             11|1896565|
|             10|1892119|
|              9|1742475|
|             14|1729441|
|             13|1702627|
|             15|1621200|
|             16|1486015|
|              8|1279326|
|             17|1257361|
|             22|1243140|
|             18|1197381|
|             21|1185272|
|             23|1139315|
|             19|1135469|
|             20|1123580|
|              7| 777523|
|              1| 623991|
|              6| 446905|
+---------------+-------+

```   
  
  
### 3.10. What is the average resolution time by agency? ###
  
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
  
  
