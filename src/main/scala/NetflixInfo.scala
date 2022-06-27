import org.apache.log4j.Logger
import org.apache.spark.sql.types.ByteType.json
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.reflect.io.Path

object NetflixInfo extends Serializable {

  val person_id= "person_id"
  val id= "id"
  val Name= "name"
  val character= "Character"
  val Role= "Role"


  def main(args: Array[String]): Unit = {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

//    if (args.length == 0) {
//      logger.info("Usage: Give file path")
//      System.exit(1)

      val spark=SparkSession.builder()
        .appName("Netflix Information")
        .master("local[3]")
        .getOrCreate()

      val ReadFile=NetflixDF(spark,"Data/credits.csv",NetflixSchema())
      ReadFile.show(10)

        val transf = transform(ReadFile)


    }
     def transform(dataF: DataFrame): RelationalGroupedDataset ={
       val data=dataF.groupBy("role")
       data

     }

     def NetflixSchema():StructType={
       StructType(List(
         StructField("person_id",IntegerType),
         StructField("id",StringType),
         StructField("Name",StringType),
         StructField("Character",StringType),
         StructField("Role",StringType)
       ))
     }

      def NetflixDF(spark:SparkSession,dataFile:String,schema:StructType): DataFrame={
        spark.read
          .format("csv")
          .option("Header","True")
          .schema(schema)
          .load(dataFile)
      }

     def writerDataDF(dframe: DataFrame, urlpath: String): Unit = {
       dframe.write
         .format("json")
//         .option("path", urlpath)
//         .option("dbtable", tableName)
         .mode(SaveMode.Overwrite)
         .save(urlpath)

     }


}
