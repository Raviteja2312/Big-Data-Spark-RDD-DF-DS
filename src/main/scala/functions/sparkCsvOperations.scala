package functions

import ReadProperties.ReadProperties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object sparkCsvOperationsSQL {


  def getSparkCsvProcessingDS(spark:SparkSession, prop:ReadProperties)={
   val csvInput = prop.getProperty("csvInput")
   // Reading CSV file[without header] which has Empty values and Renaming columns
    val inputDF = spark
                  .read
                  .option("treatEmptyValuesAsNulls", "true")
                  .option("nullValue", "null")
                  .csv(csvInput)
                  .withColumnRenamed("_c0","id")
                  .withColumnRenamed("_c1","name")
                  .withColumnRenamed("_c2","year")
                  .withColumnRenamed("_c3","rating")
                  .withColumnRenamed("_c4","movie_id")
//Persisting DF so that Analytical Operations would be quick
    inputDF.persist()
// Without an action called after Persisting the CSV data will not be on the df actually
   inputDF.take(1)

    import spark.implicits._

// Changing Datatypes of each Column
    val withDataTypes = inputDF
                        .select($"id".cast(IntegerType),$"name",$"year".cast(IntegerType),$"rating".cast(DoubleType),$"movie_id")
                        .na.fill(0.0).na.fill("")
   withDataTypes.printSchema()
// Checking whether the Blanks have been transformed to Nulls and Handled according to their Datatypes
    val check_nullCount = withDataTypes.filter($"rating".isNull)
    val check_notNullCount = withDataTypes.filter($"rating".isNotNull)

    println(check_notNullCount.count())
    println(check_nullCount.count())
    println(check_nullCount.count()+check_notNullCount.count())
    println(withDataTypes.select($"rating").count())


// Transforming DataFrame to DataSet
    val inputDS = withDataTypes.as[(Integer,String,Integer,Double,String)]
//Finding out the Highest Rated Movies according to the rating
   val highestRatedMovies = inputDS
                            .select("name","rating")
                            .groupBy("rating","name")
                            .count()
                            .orderBy(desc("rating"))

    highestRatedMovies.show(30,false)
// Finding out oldest movies list according to the year
    val oldestMovies = inputDS
                       .select("name","rating","year")
                       .groupBy("name","rating","year")
                       .count()
                       .orderBy(asc("year"))

    oldestMovies.show(5,false)

    inputDS.createOrReplaceTempView("virtualMovieDataSet")
// Finding out the Longest names in the given Movie_Names List
    val longestMovieName = spark.sql("select name ,LENGTH(name) as len from virtualMovieDataSet")
                           .orderBy(desc("len"))

    longestMovieName.show(false)
// Taking out the movie name and splitting them by splitting criteria and counting the words
    val wordCountMovieName = inputDS
                             .flatMap(_._2.toLowerCase.split("\\W+"))
                             .groupBy("value")
                             .count()
                             .orderBy(desc("count"))

    wordCountMovieName.show(false)


inputDF.unpersist()


//Returning Multiple DataFrames to the main method as tuples
   (highestRatedMovies,oldestMovies,longestMovieName,wordCountMovieName)

  }



}
