// Databricks notebook source

// DBTITLE 1,code description
// this notebook takes a JSON struct array of parameters from an ADF lookup activity. 
// with parallelism defined in a function below, it passed parameter sets to the etl-child notebook

// COMMAND ----------

// DBTITLE 1,declare variables and classes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StringType }

// define a schema for ParameterListJson  
val schema = new StructType()                   
      .add("EtlConfigKey", StringType)
      .add("SourceSchema", StringType)
      .add("SourceTable", StringType)
      .add("SourceEffectiveDateColumn", StringType)
      .add("SourceMaxEffectiveDate", StringType)
      .add("KeyColumns", StringType)
      .add("DestinationName", StringType)

// define a case class to hold the parameters as Maps for the NotebookData case class
case class paramSet(etlConfigKey: Map[String, String], // maps to etlConfigKey
                    sourceSchema : Map[String, String], // maps to SourceSchema
                   	sourceTable : Map[String, String], // maps to SourceTable
                    sourceEffectiveDateColumn : Map[String, String], // maps to SourceEffectiveDateColumn
                    sourceMaxEffectiveDate : Map[String, String], // maps to SourceMaxEffectiveDate
                    keyColumns : Map[String, String], // maps to KeyColumns
                    destinationName : Map[String, String] // maps to DestinationName
                   )

val childNotebookPath = "/Shared/etl-child.scala" 

// COMMAND ----------

// DBTITLE 1,define parallel functions
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.util.control.NonFatal

case class NotebookData(path: String, timeout: Int, parameters: Map[String, String] = Map.empty[String, String])

def parallelNotebooks(notebooks: Seq[NotebookData], numNotebooksInParallel: Integer): Future[Seq[String]] = {
  
  import scala.concurrent.{Future, blocking, Await}
  import java.util.concurrent.Executors
  import scala.concurrent.ExecutionContext
  import com.databricks.WorkflowException
  
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(numNotebooksInParallel))
  val ctx = dbutils.notebook.getContext()
  
  Future.sequence(
    notebooks.map { notebook => 
      Future {
        dbutils.notebook.setContext(ctx)
        if (notebook.parameters.nonEmpty)
          dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
        else
          dbutils.notebook.run(notebook.path, notebook.timeout)
      }
      .recover {
        case NonFatal(e) => s"ERROR: ${e.getMessage}" + "; error unhandled" 
      }
    }
  )
}

def parallelNotebook(notebook: NotebookData): Future[String] = {
  
  import scala.concurrent.{Future, blocking, Await}
  import java.util.concurrent.Executors
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.databricks.WorkflowException
  
  val ctx = dbutils.notebook.getContext()
  Future {
    dbutils.notebook.setContext(ctx)
    if (notebook.parameters.nonEmpty)
      dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
    else
      dbutils.notebook.run(notebook.path, notebook.timeout)
  }
  .recover {
    case NonFatal(e) => s"ERROR: ${e.getMessage}" + "; error unhandled" 
  }
}

// COMMAND ----------

// DBTITLE 1,get parameters, transform for parallelNotebooks(), execute
import org.apache.spark.sql.functions.{ map_concat, col, lit }

val parameterListDS = Seq(dbutils.widgets.get("ParameterListJson")).toDS() // get parameters from widget
var df = spark.read.schema(schema).json(parameterListDS) // read as dataframe with schema defined above
.map(r => paramSet(Map("etlConfigKey" -> r.getString(0)),
                   Map("sourceSchema" -> r.getString(1)), 
                   Map("sourceTable" -> r.getString(2)), 
                   Map("sourceEffectiveDateColumn" -> r.getString(3)),
                   Map("sourceMaxEffectiveDate" -> r.getString(4)),
                   Map("keyColumns" -> r.getString(5)), 
                   Map("destinationName" -> r.getString(6))))
.toDF
df = df.withColumn("parameters", map_concat(df.columns.map(col):_*))
.withColumn("path", lit(childNotebookPath)) // concatenate the maps and select the column
.withColumn("timeout", lit(60*60)) // time is in seconds, so 60 seconds per minute * 60 minutes per hour
.selectExpr("path", "timeout", "parameters")

val notebooks = df.as[NotebookData].collect.toSeq // collect to Seq, because parallelNotebooks() requires Seq[NotebookData]
val timeout = 1800

val res = Await.result( parallelNotebooks(notebooks, 5) , timeout seconds ) // execute parallelNotebooks()

// COMMAND ----------

// DBTITLE 1,get return value for ADF
var returnValue = "" // string to hold return value for ADF
res.foreach( r => returnValue = returnValue + r + ",") // for each string in res := Seq[String] concatenate the json arry
returnValue = "[" + returnValue + "]"
dbutils.notebook.exit(returnValue)
