import scala.util.{ Failure, Success, Try }
var status: (Boolean, String) = null
case class userDefinedCustomException(userDefinedException:String)  extends Exception(userDefinedException)
try {
    //some code
      
      throw new userDefinedCustomException("some language around exception")
      status = (true, "Success")
      status
    } catch {
        case e: userDefinedCustomException =>
        {
          status = (false, "Failure; " + e.getMessage)
          status
        }
        case e: someExceptionType =>
        {
          val message = e.getMessage
          message match { //handle different flavors of that exception
            case message if message.contains("a") => status = (false, message + "; a -> x")
            case message if message.contains("b") => status = (false, message + "; b -> y")
            case _ => status = (false, message)
          }
          status
        }
        case e: someOtherExceptionType =>
        {
          val message = e.getMessage
          message match { //handle different flavors of that exception
            case message if message.contains("a") => status = (false, "a -> x")
            case message if message.contains("b") => status = (false, "b -> y")
            case _ => status = (false, message)
          }
          status
        }
        case e: Throwable =>
          { //everything else...
            val message = e.getMessage
            status = (false, "Unhandled Excpetion..." + message)
         }
        status
    }

    //s"ERROR: ${e.getMessage}; "