import org.apache.log4j.Logger
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object NetflixData extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val Netflixschema = StructType(List(
      StructField("person_id",IntegerType),
      StructField("id",StringType),
      StructField("Name",StringType),
      StructField("Character",StringType),
      StructField("Role",StringType),
    ))

       if (args.length == 0) {
      logger.info("Usage: Give file path")
      System.exit(1)
    }

     logger.info("Start")
    val spark= SparkSession.builder()
      .appName("Netflix Data")
      .master("local[3]")
      .getOrCreate()


    val NetflixData = spark.read
      .format("csv")
      .option("header", "true")
     // .option("inferSchema", "true")
      .option("mode", "FAILFAST")
      .option("path", "Data/credits.csv")
      .schema(Netflixschema)
      .load()

    NetflixData.show(10)
    logger.info("CSV Schema"+NetflixData.schema.simpleString)



    logger.info("Num Partitions before: " +NetflixData.rdd.getNumPartitions)

    NetflixData.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("path", "dataSink/csv/")
     .save()

    logger.info("Finish")
     spark.stop()
  }

}