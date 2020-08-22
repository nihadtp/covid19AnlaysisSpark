package covid19
import scala.util.parsing.json.JSON

class jsonConvertor(val data: String) {

  def convert(key: String): List[Map[String, String]] = {
    val jObject = JSON.parseFull(data) match {
      case None => println("Something wrong when converting json")

      case Some(value) => value
    }
    val listOfStateData = jObject
      .asInstanceOf[Map[String, List[Map[String, String]]]](key)
    listOfStateData
  }
}
