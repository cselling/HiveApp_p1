import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

import scala.io.StdIn
import java.text.ParseException

// Project 1 - 6 Hive queries (1 assuming something about the future by looking at the trends)
//    X- users with passwords
//    X- Admin level user can add or delete other users and manipulate the data(bad design)
//     - 6 queries on data

import java.io.File

object HelloWorld {
  // Global Variables - Need to be this far outside main to avoid scope confusion
  var loggedIn = false // init to false  // flag to tell if login is correct - continues to commands
  var running = true // init to true  // flag to tell if program will continue, set to false to exit program
  var loginCredential: Row = null

  def dataInit(spark: SparkSession): Unit ={
    spark.sql("drop table if exists featuredata")
    spark.sql("drop table if exists saledata")
    spark.sql("drop table if exists storedata")
    spark.sql("drop table if exists logindata")

    spark.sql("create table if not exists featuredata(Store int,Date varchar(255),Temperature double,Fuel_Price double,MarkDown1 double,MarkDown2 double,MarkDown3 double,MarkDown4 double,MarkDown5 double,CPI double,Unemployment double,IsHoliday boolean)row format delimited fields terminated by ',';")
    spark.sql("create table if not exists saledata(Store int,Dept int,Date varchar(255),Weekly_Sales double,IsHoliday boolean)row format delimited fields terminated by ',';")
    spark.sql("create table if not exists storedata(Store int,Type varchar(1),Size bigint)row format delimited fields terminated by ',';")
    spark.sql("create table if not exists logindata(Login varchar(255), Password varchar(255), isAdmin Boolean)row format delimited fields terminated by ',';")

    spark.sql("Load data local inpath 'Data/features.csv' into table featuredata;")
    spark.sql("Load data local inpath 'Data/sales.csv' into table saledata;")
    spark.sql("Load data local inpath 'Data/stores.csv' into table storedata;")
    spark.sql("""insert into logindata values("Admin", "Password", TRUE);""")

    spark.sql("select count(*) as featureCount from featuredata;").show()
    spark.sql("select count(*) as saleCount from saledata;").show()
    spark.sql("select count(*) as storeCount from storedata;").show()
    spark.sql("select * from logindata;").show()
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Jello Jive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    dataInit(spark)
    while (running) { // flag set at the top
      print("Login: ")
      val inName = StdIn.readLine()
      val nameDB = spark.sql(s"""select * from logindata where Login = "${inName}";""")
      for(x <- nameDB) {
        if (inName == x.get(0)) {
          print("Password: ")
          val inWord = StdIn.readLine()
          if (inWord == x.get(1)) {
            loggedIn = true
            //adminCredential = x.get(2) == true // complains even though 3rd column is a bool
            loginCredential = x
          } else {
            println("Incorrect Password: ")
          }
        }
      }

      while(loggedIn) {
        print("Welcome to the Matrix: ")
        var line = StdIn.readLine()
        val commands = line.split(" ")

        commands(0).toUpperCase() match {
          case "BROWSE" => { // sends a direct query through to hive
            print("Query: select")
            val inLine = StdIn.readLine()
            try {
              spark.sql(s"select$inLine").show(2000,false)
            } catch {
              case x: org.apache.spark.sql.AnalysisException => println("incorrect query") // in case query is wrong
            }
          }
          case "QUERY1" => {
            val infoDump = "with " +
              "cte0 as (select store, sum(weekly_sales) as weekly_sales, date from saledata group by store, date), "+ // sum up department sales for same date and store
              "cte1 as (select store, round(avg(weekly_sales), 2) as AVG_SALES from cte0 group by store), "+ // average weekly sale by store
              "cte2 as (select Max(avg_sales) as Max_Sales from cte1), " +  // find the largest average
              "cte3 as (select Min(avg_sales) as Min_Sales from cte1), " + // find the smallest average
              "cte4 as (select * from cte1 join cte2 join cte3), " +       // join smallest and largest onto normal table
              "cte5 as (select store as Max_Store, avg_sales as Max_Performance from cte4 where avg_sales = max_sales), " +  // find store where largest average happened
              "cte6 as (select store as Min_Store, avg_sales as Min_Performance from cte4 where avg_sales = min_sales) "  // find store where smallest average happened
            val info1 = "select * from cte1 order by store;"  // print store's average
            spark.sql(infoDump + info1).show(1000)
            val info2 = "select * from cte5 join cte6;"   // print largest + smallest
            spark.sql(infoDump + info2).show(1000)
            println("Which store is most/least profitable\n")
          }
          case "QUERY2" => { // sends a direct query through to hive //multiline with concatenations, don't forget to put a ' ' at the end
            val infoDump = "with "+
              "cte0 as (select store, sum(weekly_sales) as weekly_sales, date from saledata group by store, date), " +  // sum all of the departments of a store to get the store's weekly sales
              "cte1 as (select cte0.store as store, Weekly_Sales, Fuel_Price from cte0 join featuredata f on cte0.Store = f.Store and cte0.date = f.date), "+ //join the misc data onto sales data
              "cte4 as (select store, avg(weekly_sales) as AVG_SALES from cte1 group by store), " + // compute normal average
              "cte2 as (select store, avg(Fuel_Price) as AVG_Fuel from featuredata  group by store), "+ // compute average fuel price per store
              "cte3 as (select cte1.store, avg_fuel, fuel_price, weekly_sales from cte1 join cte2 on cte1.store = cte2.store where Fuel_Price > AVG_Fuel), " + //filter sales data by fuel price
              "cte5 as (select store, avg(weekly_sales) as AVG_HIGH from cte3 group by store), " + // 2nd average of sales during weeks of high fuel price
              "cte6 as (select cte4.store, avg_sales, avg_high from cte4 join cte5 on cte4.store = cte5.store) "  // join two averages to compare
            val info1 = "select store, round(((avg_high/avg_sales)-1)*100, 6) as percent_change from cte6 order by store;"
              spark.sql(infoDump + info1).show(1000)
            val info2 = "select round(max((avg_high/avg_sales)-1)*100, 6) as max_change, round(min((avg_high/avg_sales)-1)*100, 6) as min_change, round(avg(((avg_high/avg_sales)-1)*100), 6) as average_change from cte6;"
              spark.sql(infoDump + info2).show(1000)
            println("Effects of High Fuel Prices\n")
          }
          case "QUERY3" => { // sends a direct query through to hive //multiline with concatenations, don't forget to put a ' ' at the end
            val infoDump = "with "+
              "cte0 as (select store, sum(weekly_sales) as weekly_sales, date from saledata group by store, date), " +  // sum all of the departments of a store to get the store's weekly sales
              "cte1 as (select cte0.store as store, Weekly_Sales, Temperature from cte0 join featuredata f on cte0.Store = f.Store and cte0.date = f.date), "+ //join the misc data onto sales data
              "cte4 as (select store, avg(weekly_sales) as AVG_SALES from cte1 group by store), " + // compute normal average
              "cte2 as (select store, avg(Temperature) as AVG_Temp from featuredata  group by store), "+ // compute average temperature per store
              "cte3 as (select cte1.store, avg_temp, Temperature, weekly_sales from cte1 join cte2 on cte1.store = cte2.store where Temperature > AVG_Temp), " + //filter sales data by temperature
              "cte5 as (select store, avg(weekly_sales) as AVG_HIGH from cte3 group by store), " + // 2nd average of sales during weeks of high Temperature
              "cte6 as (select cte4.store, avg_sales, avg_high from cte4 join cte5 on cte4.store = cte5.store) "  // join two averages to compare
            val info1 = "select store, round(((avg_high/avg_sales)-1)*100, 6) as percent_change from cte6 order by store;"
            spark.sql(infoDump + info1).show(1000)
            val info2 = "select round(max((avg_high/avg_sales)-1)*100, 6) as max_change, round(min((avg_high/avg_sales)-1)*100, 6) as min_change, round(avg(((avg_high/avg_sales)-1)*100), 6) as average_change from cte6;"
            spark.sql(infoDump + info2).show(1000)
            println("Effects of High Temperature\n")
          }
          case "QUERY4" => { // sends a direct query through to hive //multiline with concatenations, don't forget to put a ' ' at the end
            val infoDump = "with "+
              "cte0 as (select store, sum(weekly_sales) as weekly_sales, date from saledata group by store, date), " +  // sum all of the departments of a store to get the store's weekly sales
              "cte1 as (select cte0.store as store, Weekly_Sales, Unemployment from cte0 join featuredata f on cte0.Store = f.Store and cte0.date = f.date), "+ //join the misc data onto sales data
              "cte4 as (select store, avg(weekly_sales) as AVG_SALES from cte1 group by store), " + // compute normal average
              "cte2 as (select store, avg(Unemployment) as AVG_Unempl from featuredata  group by store), "+ // compute average unemployment per store
              "cte3 as (select cte1.store, avg_unempl, Unemployment, weekly_sales from cte1 join cte2 on cte1.store = cte2.store where Unemployment > AVG_unempl), " + //filter sales data by unemployment
              "cte5 as (select store, avg(weekly_sales) as AVG_HIGH from cte3 group by store), " + // 2nd average of sales during weeks of high Unemployment
              "cte6 as (select cte4.store, avg_sales, avg_high from cte4 join cte5 on cte4.store = cte5.store) "  // join two averages to compare
            val info1 = "select store, round(((avg_high/avg_sales)-1)*100, 6) as percent_change from cte6 order by store;"
            spark.sql(infoDump + info1).show(1000)
            val info2 = "select round(max((avg_high/avg_sales)-1)*100, 6) as max_change, round(min((avg_high/avg_sales)-1)*100, 6) as min_change, round(avg(((avg_high/avg_sales)-1)*100), 6) as average_change from cte6;"
            spark.sql(infoDump + info2).show(1000)
            println("Effects of High Unemployment\n")
          }
          case "QUERY5" => { // sends a direct query through to hive //multiline with concatenations, don't forget to put a ' ' at the end
            val infoDump = "with "+
              "cte1 as (select store, sum(weekly_sales) as weekly_sales, date from saledata group by store, date), " +  // sum all of the departments of a store to get the store's weekly sales
              "cte2 as (select store, avg(weekly_sales) as AVG_SALES from cte1 group by store), " + // compute normal average
              "cte3 as (select store, sum(weekly_sales) as weekly_sales, date from saledata where isholiday = true group by store, date), " +
              "cte4 as (select store, avg(weekly_sales) as HLDY_SALES from cte3 group by store), " + // compute holiday average
              "cte5 as (select cte2.store, avg_sales, hldy_sales from cte2 join cte4 on cte2.store = cte4.store) "  // join two averages to compare
            val info1 = "select store, round(((hldy_sales/avg_sales)-1)*100, 6) as percent_change from cte5 order by store;"
            spark.sql(infoDump + info1).show(1000)
            val info2 = "select round(max((hldy_sales/avg_sales)-1)*100, 6) as max_change, round(min((hldy_sales/avg_sales)-1)*100, 6) as min_change, round(avg(((hldy_sales/avg_sales)-1)*100), 6) as average_change from cte5;"
            spark.sql(infoDump + info2).show(1000)
            println("Effects of Holidays\n")
          }
          case "QUERY6" => {
            val infoDump = "with " +
              "cte0 as (select store, date, to_date(date, 'dd/MM/yyyy') as real_Date , sum(weekly_sales) as sales from saledata group by store, date order by store asc, real_date desc), " + // convert "date" to date // make a date table
              "cte1 as (select store, real_date, sales, row_number() over (partition by store order by real_date desc) as seqnum from cte0), " +                          // select top 2 dates
              "cte2 as (select * from cte1 where seqnum = 1), "+
              "cte3 as (select * from cte1 where seqnum = 2), "+
              "cte4 as (select cte2.store, round(cte3.sales, 2) as last_week_sales, cte3.real_date as last_week, round(cte2.sales, 2) as this_week_sales, cte2.real_date as this_week, round(cte2.sales-cte3.sales+cte2.sales, 2) as next_week_sales from cte2 join cte3 on cte2.store = cte3.store) "
            val info1 = "select * from cte4;"
            spark.sql(infoDump + info1).show(1000)
            println("Trends\n")
          }
          case "ADDDATA" => { // adds a new element to our sales table
            if(loginCredential.get(2) == true){
              println("Adding to Table (Store(int), Dept(int), Date(varchar(255)), Weekly_Sales(double), IsHoliday(boolean)):")
              val inString = StdIn.readLine()
              val newRow = inString.split("( |,)+")
              spark.sql(s"insert into saledata values(${newRow(0).toInt}, ${newRow(1).toInt}, ${newRow(2)}, ${newRow(3).toDouble}, ${newRow(4).toBoolean});")
            } else {
              println("Denied, please contact your System Administrator")
            }
          }
          case "ADDUSER" => { // Gets a Login, Password pair and writes it into our table of valid logins
            if (loginCredential.get(2) == true) { // needs admin status
              print("New Login: ")
              val inLog = StdIn.readLine()
              print("New Password: ")
              val inWord = StdIn.readLine()
              print("Repeat Password: ")
              val wordCheck = StdIn.readLine()
              if (inWord == wordCheck) {
                spark.sql(s"""insert into logindata values("$inLog", "$inWord", ${false});""") // cannot create a new admin for unclear reasons
                println(s"Welcome User $inLog!")
              } else {
                println("Passwords did not match, Please Try Again")
              }

            } else {
              println("Denied, please contact your System Administrator")
            }
          }
          case "DELUSER" => { // deleting is not possible with hive queries, so we create a new table using everything except the user we wish to delete
            if (loginCredential.get(2) == true) {
              print("Which user?: ")

              val inLog = StdIn.readLine()
              val tempDF = spark.sql(s"""select * from logindata where Login != "${inLog}" order by Login;""")
              val newTable = tempDF.collect() // gets an Array[Row], not an RDD

              spark.sql("drop table logindata ;") // drop old table, make new one
              spark.sql("create table logindata(Login varchar(255), Password varchar(255), isAdmin Boolean)row format delimited fields terminated by ',';")

              for(x <- newTable) { // iterate to insert each user
                spark.sql(s"""insert into logindata values("${x(0)}", "${x(1)}", ${x(2)});""")
              }
            } else { // user was not admin
              println("Denied, please contact your System Administrator")
            }
          }
          case "REINITIALIZETHEDATA" => {
            dataInit(spark)
          }
          case "LOGOUT" => { // breaks the inner while loop to allow another user to login
            loggedIn = false
            println("Signing Out...")
          }
          case "EXIT" => { // breaks out of both while loops to end program without killing it
            running = false
            loggedIn = false
            println("Signing Out...")
          }
          case default => println("Describe the operation to perform:  LOGOUT, EXIT")
        }
      }
    }
    println("Goodbye...")
  }
}
