package covid19

import org.joda.time.DateTime
import org.apache.log4j.Logger

abstract class allStatusData(
    stateCodeValueMap: Map[String, Float],
    prop: String,
    date: DateTime
) extends Serializable {
  def getProp(): String = prop
  def getDate(): String = date.toString()
  def getStateValue: Map[String, Float] = stateCodeValueMap
  def dateValue: DateTime = date

  def maxValue: Float = {
    if (stateCodeValueMap.contains("tt"))
      stateCodeValueMap.-("tt").valuesIterator.max
    else stateCodeValueMap.valuesIterator.max
  }
  def maxValueStates: List[String] = {
    var listOfTuples = scala.collection.mutable.ListBuffer[(String, Float)]()
    stateCodeValueMap.foreach(k => listOfTuples += ((k._1, k._2)))
    val reversKey = listOfTuples.toList.map(kv => (kv._2, kv._1))
    val groupedKey = reversKey.groupBy(key => key._1)
    val g = groupedKey.map(x => (x._1, x._2.map(x => x._2)))
    g.getOrElse(maxValue, List(""))
  }

  def minValue: Float = {
    if (stateCodeValueMap.contains("tt"))
      stateCodeValueMap.-("tt").valuesIterator.min
    else stateCodeValueMap.valuesIterator.min
  }

  def minValueStates: List[String] = {
    var listOfTuples = scala.collection.mutable.ListBuffer[(String, Float)]()
    stateCodeValueMap.foreach(k => listOfTuples += ((k._1, k._2)))
    val reversKey = listOfTuples.toList.map(kv => (kv._2, kv._1))
    val groupedKey = reversKey.groupBy(key => key._1)
    val g = groupedKey.map(x => (x._1, x._2.map(x => x._2)))
    g.getOrElse(minValue, List(""))
  }
}

case class Confirmed(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Confirmed", date)
case class Deceased(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Deceased", date)
case class Recovered(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Recovered", date)
case class BadData(
    stateCodeValueMap: Map[String, Float],
    prop: String,
    date: DateTime
) extends allStatusData(stateCodeValueMap, prop, date)
case class Output(
    stateCodeValueMap: Map[String, Float],
    prop: String,
    date: DateTime
) extends allStatusData(stateCodeValueMap, prop, date)

object allStatusData {
  def apply(
      stateCodeValueMap: Map[String, Float],
      prop: String,
      date: DateTime
  ): allStatusData =
    prop match {
      case "Confirmed" => new Confirmed(stateCodeValueMap, date)
      case "Deceased"  => new Deceased(stateCodeValueMap, date)
      case "Recovered" => new Recovered(stateCodeValueMap, date)
      case "BadData"   => new BadData(stateCodeValueMap, prop, date)
      case _           => new Output(stateCodeValueMap, prop, date)
    }
}
