// This utility casts data from a dataframe given a vector of column names and target types
// The real value added here is a dependable and consistent way to handle different source date formats

import org.apache.spark.sql.functions.{ udf, array, lit, col, md5, concat_ws }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._

import java.sql.Timestamp

import scala.util.{ Try, Failure, Success }
import java.text.SimpleDateFormat
import util.control.Breaks._
import scala.collection.mutable.Buffer

object CastingUtility extends Serializable {

  val inputTimestampFormats = Array("yyyy-MM-dd",
                                    "yyyyMMdd",
                                    "yyyy/MM/dd",
                                    "yyyy-MM-dd'T'HH:mm:ss'Z'",
                                    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                                    "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
                                    "yyMMddHHmmssZ",
                                    "EEE, d MMM yyyy HH:mm:ss Z",
                                    "yyyyy.MMMMM.dd GGG hh:mm aaa",
                                    "yyyy.MM.dd G 'at' HH:mm:ss z",
                                    "yyyy/MM/dd'T'HH:mm:ss'Z'",
                                    "yyyy/MM/dd'T'HH:mm:ss.SSSXXX",
                                    "yyyy/MM/dd'T'HH:mm:ss.SSSZ")
  
  // function for actually performing the casting 
  val StringtoTimestamp = (stringTimeStamp: String) => 
  {
    val resultFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'") // the data format for output
    var resultTimestamp = Timestamp.valueOf("1900-01-01 00:00:00 ") // make the conversion 

    breakable {
      for (parse <- inputTimestampFormats) {
        val sdf = new SimpleDateFormat(parse)
        Try(sdf.parse(stringTimeStamp)) match {
          case Success(value) => {
            val sdfParsed = sdf.parse(stringTimeStamp)
            val stringDate = resultFormat.format(sdfParsed)
            //val resultDate = resultFormat.parse(stringDate)
            val stringTimestamp = stringDate.toString().replace("T"," ").replace("Z","")
            resultTimestamp = Timestamp.valueOf(stringTimestamp) // make the conversion 
            break
          }
          case Failure(ex) => {
            "Fail"
          }
        }
      }
    }
    resultTimestamp
  }
  val udfStringtoTimestamp = udf(StringtoTimestamp(_:String))
  
  // test casting to specified datatype. We can then split failures off to failDF, and we can actually cast passes return passDF    
  val testCastColumns = (exceptionCol: String, castCols: Seq[String], datatypeCol: String) =>
  {
    var res = exceptionCol

    if(datatypeCol == "Timestamp") {
      castCols.foreach { col =>
       breakable {
         for (parse <- inputTimestampFormats) {
           val sdf2 = new SimpleDateFormat(parse)
           res = Try(sdf2.parse(col)) match {
             case Success(value) => {
               res = "Pass"
               break
             }
             case Failure(ex) => {
               if (col == null || col == "") res
               else "Fail"
             }
           }
         }
       }
      }
    } else {
      castCols.foreach { col =>
        res = Try( 
          datatypeCol match {
            case "IntegerType" => col.toInt
            case "DoubleType" => col.toDouble
          }
        ) match {
          case Success(value) => res
          case Failure(ex) => {
            if (col == null || col == "") res
            else "Fail"
          }
        }
      }
    }
    res
  }
  val testCastColumnsUDF = udf(testCastColumns)
  
  // takes a dataframe with Exception = Pass and performs the actual casting for DataType in (IntegerType , DoubleType, FloatType, LongType )
  def castDfPassColumns(dfPass: DataFrame, castCols: Seq[String], datatype: String) : DataFrame = {
    
    var df = dfPass
    
    if(datatype == "Timestamp") {
      
      castCols.foreach { castCol => 
        df = df.withColumn(castCol, udfStringtoTimestamp(col(castCol)))
      }

    } else {
    
      var dt : DataType = datatype match {
        case "IntegerType" => IntegerType
        case "DoubleType" => DoubleType
      }
      castCols.foreach { castCol => 
        df = df.withColumn(castCol, df(castCol).cast(dt))
      }
    }  
    df
  }
  
  //================================================================================
  // Functions to  Cast records according to DataType's using Custom UDF's defined
  //================================================================================

  def castColumns(validDF: DataFrame, listOfColumnsPerDataType: (List[String], List[String], List[String], List[String], List[String], List[String])): (DataFrame, DataFrame) = {
    
    var df = validDF.distinct.withColumn("Exception", lit("Pass")).withColumn("Hash", md5(concat_ws(",",validDF.columns.map(col):_*)))
    var passDF = df.select($"Hash") // we initialize this with only the hash column. then we'll join each datatype to this df
    var failDF = sqlContext.createDataFrame(sc.emptyRDD[Row], df.schema)
  
    val colListList = List(listOfColumnsPerDataType._1, listOfColumnsPerDataType._2, listOfColumnsPerDataType._3, listOfColumnsPerDataType._4, listOfColumnsPerDataType._5, listOfColumnsPerDataType._6)
    val dataTypeList = List("StringType", "IntegerType", "LongType", "DoubleType", "FloatType", "Timestamp")
    
    var n = dataTypeList.length
    
    for (i <- 0 to (n - 1)) { // loop through lists of datatypes and column lists
      
      val colList = colListList(i)
      val colListWithHash = "Hash" :: colList
      val dataType = dataTypeList(i)
      
      if(!colList.isEmpty) {
        
        var passFailDataTypeDF = df.withColumn("DataType", lit(dataType)) // work dataframe for each pass through loop to test casting and then subset to passDataTypeDF and failDataTypeDF
        val colListTrnsfrm = colList.map(c => passFailDataTypeDF(c))
        passFailDataTypeDF = passFailDataTypeDF.withColumn("Exception", testCastColumnsUDF(col("Exception"), array(colListTrnsfrm.distinct: _*), col("DataType"))) // test casting to datatype
        val passDataTypeDF = castDfPassColumns(passFailDataTypeDF.filter($"Exception" === "Pass"), colList, dataType ).select(colListWithHash.head, colListWithHash.tail: _*) // for Exception = Pass, perform casting   
        val failDataTypeDF = passFailDataTypeDF.filter($"Exception" === "Fail").drop("DataType") // for Exception = Fail, subset for output
        
        passDF = passDF.join(passDataTypeDF, "Hash")
        failDF = failDF.union(failDataTypeDF) // add failures to failDF
        
        }
      }
    passDF = passDF.drop("Hash")
    failDF = failDF.drop("Hash")
    (passDF, failDF)
  }

  def stringForCastingData(columnNamePosition: Int, columnTypePosition: Int, schemaDefinitionArray: Array[Row]): List[String] = {
    var listForCasting = Buffer[String]()
    schemaDefinitionArray.foreach { row =>
      val columnName = row.get(columnNamePosition).toString()
      val dataType = row.get(columnTypePosition).toString()

      dataType match {
        case "String"  => listForCasting += getCastingString(columnName, dataType)
        case "Integer" => listForCasting += getCastingString(columnName, dataType)
        case "Long"    => listForCasting += getCastingString(columnName, dataType)
        case "Double"  => listForCasting += getCastingString(columnName, dataType)
        case "Float"   => listForCasting += getCastingString(columnName, dataType)
        case _         => listForCasting += getCastingString(columnName, "String")
      }
    }

    def getCastingString(columnName: String, dataType: String): String = {
      columnName.forall(_.isLetterOrDigit) match {
        case true  => "cast( " + columnName + " as " + dataType + " ) " + columnName
        case false => "cast( " + "`" + columnName + "`" + " as " + dataType + " ) " + "`" + columnName + "`"
      }
    }
    listForCasting.toList
  }

}