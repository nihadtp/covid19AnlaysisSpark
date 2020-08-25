package covid19
import org.json4s._
import org.json4s.jackson.JsonMethods._

class jsonConvertor(val data: String) {

  
  def convert(key: String): List[Map[String, String]] = {
    implicit val formats = DefaultFormats
    val jObject = parseOpt(data) match {
      case None => println("Something wrong when converting json")

      case Some(value) => value.extract[Map[String, List[Map[String, String]]]]
    }
    val listOfStateData = jObject
      .asInstanceOf[Map[String, List[Map[String, String]]]](key)
    listOfStateData
  }
}
