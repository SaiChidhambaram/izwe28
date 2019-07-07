package org.iz.hackathon
//Comments added
//1. Import required classes:
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


//3. Define case class. The case class defines the table schema. You specify the name of the class, each field, and type. 
//4. To define the case class Incidents, complete the statement below:
case class Incidents(incidentnum:String, category:String,description:String, dayofweek:String, date:String,time:String, pddistrict:String, resolution:String,address:String, x:Double, y:Double, pdid:String)

object hack1 {
  def getMonth (dt:String):String=
  {
    return (dt.substring(0,dt.indexOf('/')));
  }

  def main (args: Array[String])
  {
    val spark=SparkSession.builder().enableHiveSupport.appName("Hackathon 1").master("local[*]").getOrCreate();
    val sc = spark.sparkContext;
    sc.setLogLevel("ERROR");
    
    val sqlc = spark.sqlContext;
    import sqlc.implicits._
          
    //2. Create a Dataframe to Load the data from the file sfpd.csv using csv option  
    val sfpdDF = sqlc.read
                     .option("header","false")  
                     .option("inferschema","true")
                     .option("delimiter",",")
                     .csv("hdfs://localhost:54310/user/hduser/data/sfpd.csv")
                     .toDF("incidentnum", "category", "description", "dayofweek", "date", "time", "pddistrict", "resolution", "address", "X", "Y", "pdid")
    //sfpdDF.printSchema
    //sfpdDF.show(10,false)
                     
    println("The count of records loaded in the data frame is : " + sfpdDF.count());
    
    //5. Convert the DataFrame into a Dataset of Incidents using the above case class with the “as” method:
    val sfpdDS =  sfpdDF.as[Incidents];
    //sfpdDS.printSchema
    
    //6. Register the Dataset as a table called sfpd.
    sfpdDS.createOrReplaceTempView("sfpd");
    
    //7. What are the five districts with the most number of incidents?
    val incByDistSQL = spark.sql("select district, inscount from  (select pddistrict district, count(incidentnum) inscount  from sfpd group by pddistrict) order by inscount desc limit 5");
    println("Incident count wise top 5 districts")
    println("-----------------------------------");
    incByDistSQL.show(false);
    
    //8. What are the top 10 resolutions of incidents?
    val top10ResSQL = spark.sql("select resolution, inscount from (select resolution,count(incidentnum) inscount from sfpd group by resolution) order by inscount desc  limit 10");
    println("Incident count wise top 10 resolutions")
    println("--------------------------------------");
    top10ResSQL.show(false);
    
    //9. What are the top three count of categories of incidents?
    val top3CatSQL = spark.sql("select category, inscount from (select category,count(incidentnum) inscount from sfpd group by category) order by inscount desc  limit 3");
    println("Incident count wise top 3 incident categories")
    println("---------------------------------------------");
    top3CatSQL.show(false);
    
    //10. Save the top 10 resolutions to a JSON file with proper alias name in the folder /user/hduser/sfpd_json/.
    top10ResSQL.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sfpd_json/");
    
    //11. Identify on the data which contains “WARRANTS” and load into HDFS /user/hduser/sfpd_parquet/ in parquet format.
    val warrantsResSQL = spark.sql("select incidentnum, category, description, dayofweek, date, time, pddistrict, resolution, address, X, Y, CAST(pdid AS String) from sfpd where category = 'WARRANTS'");
    //warrantsResSQL.printSchema
    warrantsResSQL.write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/sfpd_parquet/");
    
    //12. Register the UDF and define function that extracts characters after the last ‘/’ from the string:
    spark.udf.register("getyear", (inputdt:String)=>{inputdt.substring(inputdt.lastIndexOf('/')+1) });
    
    
    //13.  Using the registered the UDF in a SQL query, find the count of incidents by year and show the  results:
    val incyearSQL = spark.sql("SELECT getyear(date) AS year, count(incidentnum) AS incidentCount FROM sfpd GROUP BY getyear(date) ORDER BY incidentCount DESC");
    println("the count of incidents by year");
    println("------------------------------");
    incyearSQL.show(false);
    
    
    //14. Find and display the category, address, and resolution of incidents reported in 2014:
     val inc2014 = spark.sql("select category, address,  resolution from sfpd where getyear(date) =14 ")
     println("the category, address, and resolution of incidents reported in 2014");
     println("-------------------------------------------------------------------");
     inc2014.show(false);

     
    //15. Find and display the addresses and resolutions of vandalism incidents in 2015:
     val van2015 = spark.sql("select address , resolution from sfpd where  category = 'VANDALISM' and getyear(date) =15");
     println("the addresses and resolutions of vandalism incidents in 2015");
     println("------------------------------------------------------------");
     van2015.show(false);
     
     //Try creating other functions. For example, a function that extracts the month, and use this to see which month in 2014 had the most incidents.
     spark.udf.register("getMonth",getMonth _)
     val van2014 = spark.sql("select getMonth(date) Month,count(incidentnum) IncidentCount from sfpd where getyear(date) =14 group by getMonth(date) order by count(incidentnum) desc limit 1");
     println("month in 2014 that had the most incidents");
     println("-----------------------------------------");
     van2014.show(false);
      println("-----Program Ends---------");
  }
}