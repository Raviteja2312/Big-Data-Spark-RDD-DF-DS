import ReadProperties.ReadProperties
import functions.sparkCsvOperationsSQL
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object sparkProgram {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
// Using a Property file as an argument to the main method
    val propertyFile = args(0)

    val prop = new ReadProperties
    prop.setPropertyFile(propertyFile)

    val jsonOut = prop.getProperty("jsonOut")

    val conf = new SparkConf()
    val spark = SparkSession.builder.config(conf).appName("Spark Operations").master("local").getOrCreate()

    /* Writing a Method and passing parameters and returning multiple dataframes as tuples */
    val DS_Out = sparkCsvOperationsSQL.getSparkCsvProcessingDS(spark,prop)

   /* Writing the 4 Dataframes(_1,_2,_3,_4) to one JSON file with AppendMode*/

    DS_Out._1.coalesce(1).write.mode("append").json(jsonOut)
    DS_Out._2.coalesce(1).write.mode("append").json(jsonOut)
    DS_Out._3.coalesce(1).write.mode("append").json(jsonOut)
    DS_Out._4.coalesce(1).write.mode("append").json(jsonOut)



  }




}
