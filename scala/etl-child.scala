// Databricks notebook source
// DBTITLE 1,code description
// this notebook performed the work on merging the stage data with the active data,
// given a set of key columns, maz effective date field and value, and other parameters

// COMMAND ----------

// DBTITLE 1,get/assign variables
import java.sql.Timestamp //def nowTime

val startTime = System.nanoTime // for logging ProcessSeconds

val etlConfigKey = dbutils.widgets.get("etlConfigKey")
val sourceSchema = dbutils.widgets.get("sourceSchema")
val sourceTable = dbutils.widgets.get("sourceTable")
var keyColumns = dbutils.widgets.get("keyColumns")
keyColumns = keyColumns.replace("]","").replace("[","")
val sourceEffectiveDateColumn = dbutils.widgets.get("sourceEffectiveDateColumn")
val sourceMaxEffectiveDate = dbutils.widgets.get("sourceMaxEffectiveDate")

val databricksTableName = "cdh." + sourceTable
val basePath = "/mnt/AzureSqlServers/serverName/databaseName/"
val stagePath = basePath + sourceSchema + "/" + sourceTable + "/" + "Stage"
val activePath = basePath + sourceSchema + "/" + sourceTable + "/" + "Active"

var RowsInserted : Long = 0
var RowsUpdated : Long = 0
var MaxEffectiveDate : Timestamp = null

case class EtlProcessLogRecord(CreatedOn: String, 
                               DestinationName: String, 
                               RowsInserted: String, 
                               RowsUpdated: String, 
                               ProcessSeconds: String, 
                               ErrorMessage: String,
                               MaxEffectiveDate: String,
                               etlConfigKey: String
                              )

// COMMAND ----------

// DBTITLE 1,define a utility for working with files
import scala.util.{ Failure, Success, Try }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, FileSystem, FileUtil, Path }
import org.apache.spark.sql.DataFrame // to initialize df var below, and for display df function
import org.apache.spark.sql.types._

object HelperFunctions extends Serializable {
  
  // for use in hasFilesInFolder
  def getHadoopFileSystemClient(): FileSystem = { 
    var fs: FileSystem = null
    val conf = new Configuration()
    if (fs == null)
      fs = FileSystem.get(conf)
    else fs
    fs
  }
  
  // if no files in Active folder (initial load) we want to only query Stage folder so as to avoid an error re: reading schema; also, if no files in stage, job is complete
  def hasFilesInFolder(fs: FileSystem, path: String): Boolean = { 
    var res : Boolean = false
    val p = new Path(path)
    val status: Array[FileStatus] = fs.listStatus(p)
    if (status.length > 0) res = true
    res
  }

  // A function for printing the first row of a data frame in a friendly way
  def kvpFirstDataFrameRow(df: DataFrame) : String = {
    var firstRow : String = "DataFrame is Empty!!!"
    if(!df.head(1).isEmpty) {
      val cols = df.columns.mkString(",").split(",")
      val firstRowValues = df.head.toString.replace("[","").replace("]","").split(",")
      firstRow = cols.zip(firstRowValues).map(x => x).mkString(", ")
    }
    return(firstRow)
  }
  
  // spark doesn't support most datetime encoders. This function gets a java.sql.Timestamp to use as the "CreatedOn" date in etl.ProcessLog
  def nowTime() : java.sql.Timestamp = { // get current timestamp
    // example call: 
    //   val now = nowTime()
    // returns: 
    //   now: java.sql.Timestamp = 2019-04-16 20:15:21.99
    import java.time.LocalDateTime //def nowTime
    val l = LocalDateTime.now() // get now in LocalDateTime
    val s = l.toString().replace("T"," ").replace("Z","") // convert to string and format for conversion to java.sql.Timestamp
    val t = Timestamp.valueOf(s) // make the conversion 
    return t
  }
  
  def getEtlProcessLogRecord(RowsInserted: String, RowsUpdated: String, ErrorMessage: String, MaxEffectiveDate: String, etlConfigKey: String ) : EtlProcessLogRecord = {
    
    val CreatedOn = HelperFunctions.nowTime().toString
    val ProcessSecondsDouble = ( System.nanoTime - startTime ) / 1e9d
    val ProcessSeconds = BigDecimal(ProcessSecondsDouble).setScale(2, BigDecimal.RoundingMode.DOWN).toString
    val DestinationName = activePath.replace("/mnt","")
    
    val etlProcessLogRecord = EtlProcessLogRecord(CreatedOn, DestinationName, RowsInserted, RowsUpdated, ProcessSeconds, ErrorMessage, MaxEffectiveDate, etlConfigKey)
    etlProcessLogRecord
  }

}

// COMMAND ----------

// DBTITLE 1,define an object to store etl functions
import org.apache.spark.sql.functions.{ lit, desc, row_number, max } // lit for adding Stage and Active fields to df; desc and row_number used to get latest record
import org.apache.spark.sql.expressions.Window // used to get latest record

object ETL extends Serializable {

  // Step 1 is to read the stage and active files, find the latest record for each Key, and re-write to active
  def readStageAndActiveWriteToActive() : (Boolean, String) = {
    println("Begin readStageAndActiveWriteToActive")
    var status: (Boolean, String) = null
    var currentRecordCount: Long = 0
    try {
      
      val dfStage = spark.read.parquet(stagePath).withColumn("SourceFolder", lit("Stage")) // get files from Stage (just written from adf); SourceFolder column used to identify inserts vs updates
      val keyColList = keyColumns.split(",") // possible for there to exist more than one key column. These are passed from polaris_customerHub.etl.Remediation as a comma-separated string
      
      if(sourceMaxEffectiveDate.startsWith("1900-01-01")){ // if the sourceMaxEffectiveDate is 1900-01-01, it could be because we are trying to reload the table. 
                                                              //In that case, truncating the active folder will accomplish this.
        removeAndMakePath(activePath)
      }
      val hasFilesInActiveFolder = HelperFunctions.hasFilesInFolder(HelperFunctions.getHadoopFileSystemClient(), activePath) // check Active folder for files

      var df : DataFrame =
      hasFilesInActiveFolder match {
        case true => { // if there are files in Active, union with Stage and find latest
          println("readStageAndActiveWriteToActive - hasFilesInActiveFoder: true")
          val dfActive = spark.read.parquet(activePath).withColumn("SourceFolder", lit("Active")) // get files from Active (all current records)
          currentRecordCount = dfActive.count()
          val df = dfStage
          .union(dfActive) // union Stage and Active and identify changes, using the tables key column and effective date
          .withColumn("RowN", row_number().over(Window.partitionBy(keyColList.head, keyColList.tail: _*).orderBy(desc(sourceEffectiveDateColumn))))
          df
        }
        case false => { // if there are not files in Active, find latest from stage (use case is a re-start of the ETL, or dups in source)
          println("readStageAndActiveWriteToActive - hasFilesInActiveFoder: false")
          val df = dfStage.withColumn("RowN", row_number().over(Window.partitionBy(keyColList.head, keyColList.tail: _*).orderBy(desc(sourceEffectiveDateColumn))))
          df
        }
      }

      RowsUpdated = df.filter($"SourceFolder" === "Active").filter($"RowN" === 2).count() // rows updated will have an Active record with RowN = 2
      println("readStageAndActiveWriteToActive - RowsUpdated = " + RowsUpdated)
      
      RowsInserted = df.filter($"RowN" === 1).count() - currentRecordCount // rows inserted are the rows in the df we'll be inserting minus the rows that were there before
      println("readStageAndActiveWriteToActive - RowsInserted = " + RowsInserted)
      
      MaxEffectiveDate = df.select(max(sourceEffectiveDateColumn)).first.getTimestamp(0) // building in an assumption that the data is Timestamp. This is good as long as...
      // ...the data sent to the stage folder is Timestamp, which it should be

      df
        .drop("RowN") // column not a part of source ddl
        .drop("SourceFolder") // column not a part of source ddl
        .filter($"RowN" === 1) // filter active records
        .write.mode("overwrite").parquet(activePath) // overwrite Active path
      
      status = (true, "End readStageAndActiveWriteToActive; Successful for " + sourceTable)
      status
      
    } catch {
      case e: Throwable =>
      { 
        status = (false, "End readStageAndActiveWriteToActive; Unhandled Excpetion..." + e.getMessage)
      }
      status
    }
  }
  
  // Step 2 is to write the active data to a databricks managed table 
  def overwriteDatabricksTable() : (Boolean, String) = {
    println("Begin overwriteDatabricksTable")
    var status: (Boolean, String) = null
    try {
      
      spark
        .read.parquet(activePath)
        .write.mode("overwrite").saveAsTable(databricksTableName)

      status = (true, "End overwriteDatabricksTable; Successful for " + sourceTable)
      status

    } catch {
      case e: Throwable =>
      { 
        status = (false, "End overwriteDatabricksTable; Unhandled Excpetion..." + e.getMessage)
      }
      status
    }
  }
  
  // Step 3 is to remove and make the stage path (we "trunc and load" stage, but more efficient to load then trunc)
  def removeAndMakePath(stringPath: String) : (Boolean, String) = {
    
    println("Begin removeAndMakePath")
    var status: (Boolean, String) = null
    try {
      
      val conf = new Configuration()
      var fs: FileSystem = FileSystem.get(conf)
      val path = new Path(stringPath)
      dbutils.fs.rm(stringPath, true)
      fs.mkdirs(path)
      
      status = (true, "End removeAndMakePath; Successful for " + sourceTable)
      status
    
    } catch {
      case e: Throwable =>
      { 
        status = (false, "End removeAndMakePath; Unhandled Excpetion..." + e.getMessage)
      }
      status
    }
  }
}

// COMMAND ----------

// DBTITLE 1,perform etl
import scala.util.{ Try, Failure, Success }
var status: (Boolean, String) = null
var etlProcessLogRecord : EtlProcessLogRecord = null
// first, check to see if there are files in the stage folder
println("Begin " + sourceSchema + "." + sourceTable)
val hasFilesInStageFolder = HelperFunctions.hasFilesInFolder(HelperFunctions.getHadoopFileSystemClient(), stagePath)
println("hasFilesInStageFolder: " + hasFilesInStageFolder)
hasFilesInStageFolder match {
  case true => {
    status = ETL.readStageAndActiveWriteToActive() 
    status._1 match {
      case true => {

        println(status._2)
        status = ETL.overwriteDatabricksTable()

        status._1 match {
          case true => {

            println(status._2)
            status = ETL.removeAndMakePath(stagePath)

            status._1 match {
              case true => {

                println(status._2)
                etlProcessLogRecord = HelperFunctions.getEtlProcessLogRecord(RowsInserted.toString, RowsUpdated.toString, null, MaxEffectiveDate.toString, etlConfigKey)
                println("Process Complete for " + sourceTable)
                
              }
              case false => {
                etlProcessLogRecord = HelperFunctions.getEtlProcessLogRecord(RowsInserted.toString, RowsUpdated.toString, status._2, MaxEffectiveDate.toString, etlConfigKey)
                //throw new Exception(status._2)
              }
            }
          }
          case false => {
            etlProcessLogRecord = HelperFunctions.getEtlProcessLogRecord(RowsInserted.toString, RowsUpdated.toString, status._2, MaxEffectiveDate.toString, etlConfigKey)
            //throw new Exception(status._2)
          }
        }
      }
      case false => {
        etlProcessLogRecord = HelperFunctions.getEtlProcessLogRecord(RowsInserted.toString, RowsUpdated.toString, status._2, MaxEffectiveDate.toString, etlConfigKey)
        //throw new Exception(status._2)
      }
    }
  }
  case false => {
    println("Process Complete: No files in Stage for " + sourceTable)
    etlProcessLogRecord = HelperFunctions.getEtlProcessLogRecord("0", "0", null, null, etlConfigKey)
  }
}

// COMMAND ----------

// DBTITLE 1,return value to master
// Import jackson json libraries
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
// Create a json serializer
val jsonMapper = new ObjectMapper with ScalaObjectMapper
jsonMapper.registerModule(DefaultScalaModule)

// Exit with json
val returnValue = jsonMapper.writeValueAsString(etlProcessLogRecord)
dbutils.notebook.exit(returnValue)
