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

  // Return max value among given states for a given property on a given date.
  def maxValue: Float = stateCodeValueMap.valuesIterator.maxBy(x => x match {
      case Float.NaN => Float.NegativeInfinity
      case _ => x
    })

  // Return List of states that returns max value for a given property on a given date.

  def maxValueStates: List[String] = {
    var listOfTuples = scala.collection.mutable.ListBuffer[(String, Float)]()

    // Converting Map into Tuple of (String, Float)
    stateCodeValueMap.foreach(k => listOfTuples += ((k._1, k._2)))

    // Reversing key value pair from (String, Float) to (Float, String)
    val reversKey = listOfTuples.toList.map(kv => (kv._2, kv._1))
    val groupedKey = reversKey.groupBy(key => key._1)

    // Group By Key and determining List of states that has maximum value
    val g = groupedKey.map(x => (x._1, x._2.map(x => x._2)))
    g.getOrElse(maxValue, List(""))
  }

  // Return min value among states among all the states for a given property on a given date.
  def minValue: Float = stateCodeValueMap.valuesIterator.minBy(x => x match {
      case Float.NaN => Float.PositiveInfinity
      case _ => x
    })

  //Return List of states that has min value for a given property on a given date.
  def minValueStates: List[String] = {
    var listOfTuples = scala.collection.mutable.ListBuffer[(String, Float)]()

    // Converting Map into Tuple of (String, Float)
    stateCodeValueMap.foreach(k => listOfTuples += ((k._1, k._2)))

    // Reversing key value pair from (String, Float) to (Float, String)
    val reversKey = listOfTuples.toList.map(kv => (kv._2, kv._1))
    val groupedKey = reversKey.groupBy(key => key._1)

    // Group By Key and determining List of states that has minimum value
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
case class Positive(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Positive", date)
case class Negative(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Negative", date)
case class Population(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "Population", date)
case class InQuarantine(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "InQuarantine", date)
case class ReleasedFromQuarantine(
    stateCodeValueMap: Map[String, Float],
    date: DateTime
) extends allStatusData(stateCodeValueMap, "ReleasedFromQuarantine", date)
case class TotalTested(stateCodeValueMap: Map[String, Float], date: DateTime)
    extends allStatusData(stateCodeValueMap, "TotalTested", date)
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
      case "Confirmed"    => new Confirmed(stateCodeValueMap, date)
      case "Deceased"     => new Deceased(stateCodeValueMap, date)
      case "Recovered"    => new Recovered(stateCodeValueMap, date)
      case "Positive"     => new Positive(stateCodeValueMap, date)
      case "Negative"     => new Negative(stateCodeValueMap, date)
      case "Population"   => new Population(stateCodeValueMap, date)
      case "InQuarantine" => new InQuarantine(stateCodeValueMap, date)
      case "ReleasedFromQuarantine" =>
        new ReleasedFromQuarantine(stateCodeValueMap, date)
      case "TotalTested" => new TotalTested(stateCodeValueMap, date)
      case _             => new Output(stateCodeValueMap, prop, date)
    }
}
