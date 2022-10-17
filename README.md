# bigdataprogram-applaudo
A repository for the Big Data Program - Applaudo Studios - Valentina Jaramillo Raquejo
Notes: The dataset that is being used for the project is https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9 . This dataset is stored in the directory dataset_test/src/main/resources/data locally.

## Exploratory Data Analysis

### Questions ### 

1. Which agencies have the highest number of service requests?
2. What are the most recurrent types of complaints in the agencies with the most service requests?
3. Which cities have the highest number of service requests?
4. Which boroughs have the highest number of service requests?
5. What are the most common channels through which service requests are made?
6. How has the channel evolved in the last years?
7. Which are the boroughs and zipcodes that have the highest number of noise complaints?
8. Which are the boroughs and zipcodes that have the highest number of drug activity complaints?
9. What is the hour of the day with the most service requests?
10. What is the average resolution time by agency?


#### Question 1  ####

|agency_name                                       |count  |  
| --- | --- | 
|New York City Police Department                   |8943589|  
|Department of Housing Preservation and Development|7421521|  
|Department of Transportation                      |3680622|  
|Department of Sanitation                          |3644155|  
|Department of Environmental Protection            |2176187|  
|Department of Buildings                           |1419574|  
|Department of Parks and Recreation                |1320413|  
|Department of Health and Mental Hygiene           |766883 |  
|Taxi and Limousine Commission                     |314378 |  
|Department of Consumer Affairs                    |268304 |  
|Department of Homeless Services                   |140381 |  
|HRA Benefit Card Replacement                      |113953 |  
|Correspondence Unit                               |90381  |  
|Department for the Aging                          |89570  |  
|Senior Citizen Rent Increase Exemption Unit       |87622  |  
|Refunds and Adjustments                           |82998  |  
|Operations Unit - Department of Homeless Services |79372  |  
|DHS Advantage Programs                            |73021  |  
|Mayorâs Office of Special Enforcement           |70955  |  
|Economic Development Corporation                  |66636  |  

only showing top 20 rows  
  
#### Question 2  ####

***New York City Police Department  

 
|complaint_type         |count  |  
| --- | --- | 
|Noise - Residential    |2889911|  
|Illegal Parking        |1731749|  
|Blocked Driveway       |1306452|  
|Noise - Street/Sidewalk|994535 |  
|Noise - Commercial     |493605 |  

  
***Department of Housing Preservation and Development  

 
|complaint_type      |count  |  
| --- | --- | 
|HEAT/HOT WATER      |1757917|  
|HEATING             |887720 |  
|PLUMBING            |840512 |  
|UNSANITARY CONDITION|675271 |  
|GENERAL CONSTRUCTION|500821 |  
 
only showing top 5 rows  
  
***Department of Transportation  


|complaint_type          |count  |  
| --- | --- |
|Street Condition        |1155611|  
|Street Light Condition  |1080133|  
|Traffic Signal Condition|533054 |  
|Sidewalk Condition      |319892 |  
|Broken Muni Meter       |172113 |  
 
only showing top 5 rows  
  
#### Question 3  ####

|city         |count  |  
| --- | --- |
|BROOKLYN     |9208752|  
|NEW YORK     |5778849|  
|BRONX        |5739577|  
|null         |1857054|  
|STATEN ISLAND|1547592|  
 
only showing top 5 rows  

#### Question 4  ####

|borough      |count  |  
| --- | --- | 
|BROOKLYN     |9270709|  
|QUEENS       |7264167|  
|MANHATTAN    |6046702|  
|BRONX        |5769470|  
|STATEN ISLAND|1573200|  

only showing top 5 rows  

#### Question 5  ####

|open_data_channel_type|count   |  
| --- | --- |  
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
  
only showing top 20 rows  


#### Question 6  ####
  
|created_year|open_data_channel_type|  count|  
| --- | --- | --- |  
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
 


#### Question 7  ####
  
|borough      |count  |  
| --- | --- |  
|MANHATTAN    |1624180|  
|BROOKLYN     |1486861|  
|BRONX        |1326379|  
|QUEENS       |997733 |  
|STATEN ISLAND|135684 |  
  
only showing top 5 rows  

  
|incident_zip|count |  
| --- | --- |  
|10466       |237859|  
|10031       |103588|  
|11226       |97768 |  
|10032       |92342 |  
|10034       |88167 |  
  
only showing top 5 rows  

#### Question 8  ####
  
|borough      |count|  
| --- | --- |  
|MANHATTAN    |7096 |  
|QUEENS       |6269 |  
|BROOKLYN     |4311 |  
|BRONX        |3706 |  
|STATEN ISLAND|698  |  
  
only showing top 5 rows  

|incident_zip|count|  
| --- | --- |  
|10035       |2545 |  
|11432       |1238 |  
|10472       |672  |  
|11213       |667  |  
|11366       |614  |  
  
only showing top 5 rows  


#### Question 9  ####

|hour_of_the_day|  count|  
| --- | --- |  
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
  
only showing top 20 rows  
  
  
#### Question 10  ####
  
|agency                                 |avg_resolution_time_in_days|  
| --- | --- |  
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
  
  
