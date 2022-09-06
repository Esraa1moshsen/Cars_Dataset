package example
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object Main extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder().appName("Read_CSV").master("local").getOrCreate()
  // read CSV file and convert it into dataFrame
  val df = sparkSession.read.options(Map("InferSchema" -> "true","delimiter" -> ",","header" -> "true")).csv("E:\\House_Rent_Scala\\src\\main\\resources\\cars_multiple_linearR.csv")

  ///////////////////////////////// pre-processing Dataset
  // print Schema (name : dataType : constraints)
  df.printSchema()

  // select one column from dataFrame
  df.select("Brand").show

  // select all columns and make castings for specific olumns
  var df_cast = df.select(
    df("Brand"),
    df("Price").cast("float"),
    df("Body"),
    df("Mileage"),
    df ("EngineV").cast("float"),
    df ("Engine_Type"),
    df ("Registration"),
    df ("Year"),
    df ("Model")
  )

  // print Schema again after casting
  df_cast.printSchema()

  // drop all  NULL rows from dataFrame
  df_cast = df_cast.na.drop()

  // show the dataFrame after dropping the NULL rows
  df_cast.show()

  // Create a temporary view
  df_cast.createOrReplaceTempView("cars_dataset")

  /////////////////////////////// Analyzing Dataset

    // 1st Query "Count Models for each Brand"
      // SQL
      sparkSession.sql("SELECT Brand, COUNT(Brand) FROM(SELECT Brand, Model  FROM cars_dataset GROUP BY Brand ,Model) GROUP BY Brand").show()
      // API
      var df_cast_query1 = df_cast.select("Brand","Model").groupBy("Brand","Model").count()
      df_cast_query1 = df_cast_query1.select("Brand").groupBy("Brand").count()
      df_cast_query1.show()

    // 2nd Query "The Best Car in each Brand according to (Price and Mileage)"
      // SQL min price  -> where condition -> select rows more than by Mileage
      var df_cast_sql2 = sparkSession.sql("SELECT Brand ,MIN(Price) AS Min_Price FROM cars_dataset GROUP BY Brand")
      df_cast_sql2.createOrReplaceTempView("brands_dataset")
      sparkSession.sql("SELECT b.Brand, c.Brand, c.Mileage ,b.Min_Price , c.Price From cars_dataset AS c LEFT JOIN brands_dataset AS b ON b.Brand = c.Brand AND b.Min_Price = c.Price WHERE b.Min_Price IS NOT NULL").show()
      sparkSession.sql("SELECT * FROM cars_dataset WHERE Brand = 'BMW' AND Price = 1400").show()
      // API
      var df_cast_query2 = df_cast.select("Brand","Price").groupBy("Brand").min("Price")
      var df_cast_query2_1 = df_cast.join(df_cast_query2,df_cast("Brand") === df_cast_query2("Brand") && df_cast("Price") === df_cast_query2("min(Price)") ,"left").filter(col("min(Price)").isNotNull)
      df_cast_query2_1.show()

    // 3rd Query "Count Model after 2015 in each Brand"
      // SQL
      var df_cast_sql3 = sparkSession.sql("SELECT Brand , COUNT(Model) FROM (SELECT Brand , Model FROM cars_dataset WHERE Year >= 2015 GROUP BY Brand , Model) GROUP BY Brand")
      df_cast_sql3.show()
      // API
      var df_cast_query3 = df_cast.select("*").filter(col("Year")>= 2015)
      var df_cast_query3_1 = df_cast_query3.select("Brand","Model").groupBy("Brand" , "Model").count()
      var df_cast_query3_2 = df_cast_query3_1.select("Brand").groupBy("Brand").count()
      df_cast_query3_2.show()

    // 4th Query "Get the Brand that contains less than 10 models after 2020"
      // SQL
      var df_cast_sql4= sparkSession.sql("SELECT Brand , COUNT(Model) FROM (SELECT Brand , Model FROM cars_dataset WHERE Year >= 2016 GROUP BY Brand , Model) GROUP BY Brand HAVING COUNT(Model) < 10")
      df_cast_sql4.show()
      // API
      var df_cast_query4 = df_cast.select("*").filter(col("Year") >= 2016)
      var df_cast_query4_1 = df_cast_query4.select("Brand","Model").groupBy("Brand" , "Model").count()
      var df_cast_query4_2 = df_cast_query4_1.select("Brand").groupBy("Brand").count().filter(col("count") < 10)
      df_cast_query4_2.show()

}