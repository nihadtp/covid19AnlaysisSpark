package covid19

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.log4j.Logger

object timeUtil {
    val formatter = DateTimeFormat.forPattern("dd-MMM-YY")
}

abstract class dataStructure(mapObject: Map[String, String]) extends Serializable {
    
    var stateInfo: scala.collection.mutable.Map[String, Float]
    val date: DateTime
    val status : String         
    }

class stateStatus(val mapObject: Map[String, String]) extends dataStructure(mapObject){
    var stateInfo = scala.collection.mutable.Map[String, Float]()
    val date = timeUtil.formatter.parseDateTime(mapObject("date"))
    val status = mapObject("status")
    for ((state, value) <- mapObject)
    {
        if ((state != "date" && state != "status"))
        stateInfo += (state -> value.toFloat)
    }
}

