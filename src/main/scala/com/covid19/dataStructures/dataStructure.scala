package covid19

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.apache.log4j.Logger
import com.covid19.dataStructures.stateNameConstants._
import scala.collection.mutable

object timeUtil {
  val formatterStatesDaily = DateTimeFormat.forPattern("dd-MMM-YY")
  val formatterTestsDaily = DateTimeFormat.forPattern("dd/MM/YYYY")
}

abstract class dataStructure(mapObject: Map[String, String])
    extends Serializable {

  val stateCodeMapper: scala.collection.immutable.Map[String, String]
  var stateInfo: scala.collection.mutable.Map[String, Float]
  val date: DateTime
  val status: String
}

class stateStatus(val mapObject: Map[String, String])
    extends dataStructure(mapObject) {

  val stateCodeMapper = Map(
    "an" -> AndamanAndNicobarIslands,
    "ap" -> AndhraPradesh,
    "ar" -> ArunachelPradesh,
    "as" -> Assam,
    "br" -> Bihar,
    "ch" -> Chandigarh,
    "ct" -> Chhattisgarh,
    "dd" -> DamanAndDiu,
    "dl" -> Delhi,
    "dn" -> DadraAndNagarHaveli,
    "ga" -> Goa,
    "gj" -> Gujarat,
    "hp" -> HimachalPradesh,
    "hr" -> Haryana,
    "jh" -> Jharkhand,
    "jk" -> JammuAndKashmir,
    "ka" -> Karnataka,
    "kl" -> Kerala,
    "la" -> Ladakh,
    "ld" -> Lakshadweep,
    "mh" -> Maharashtra,
    "ml" -> Meghalaya,
    "mn" -> Manipur,
    "mp" -> MadhyaPradesh,
    "mz" -> Mizoram,
    "nl" -> Nagaland,
    "or" -> Orissa,
    "pb" -> Punjab,
    "rj" -> Rajasthan,
    "sk" -> Sikkim,
    "tg" -> Telangana,
    "tn" -> TamilNadu,
    "tr" -> Tripura,
    "ut" -> UttarPradesh,
    "up" -> UttarPradesh,
    "wb" -> WestBengal
  )
  var stateInfo = scala.collection.mutable.Map[String, Float]()
  val date = timeUtil.formatterStatesDaily.parseDateTime(mapObject("date"))
  val status = mapObject("status")
  for ((state, value) <- mapObject) {
    if (
      (state != "date" && state != "status" && state != "tt" && state != "un")
    )
      stateInfo += (stateCodeMapper.getOrElse(
        state,
        "Unknown State Code"
      ) -> value.toFloat)
  }
}

class stateTestDaily(val mapObject: Map[String, String])
    extends dataStructure(mapObject) {

  private def getTestMapData(key: String): Float = {
    val value = mapObject.get(key)
    value match {
      case None => Float.NaN
      case Some(value) =>
        if (value.matches("[-+]?\\d*\\.?\\d*") && !value.isEmpty())
          value.toFloat
        else Float.NaN
    }
  }

  var stateInfo: mutable.Map[String, Float] = mutable.Map(
    "positive" -> getTestMapData("positive"),
    "negative" -> getTestMapData("negative"),
    "population" -> getTestMapData("populationncp2019projection"),
    "inQuarantine" -> getTestMapData("totalpeoplecurrentlyinquarantine"),
    "releasedFromQuarantine" -> getTestMapData(
      "totalpeoplereleasedfromquarantine"
    ),
    "totalTested" -> getTestMapData("totaltested")
  )
  
  val date = timeUtil.formatterTestsDaily.parseDateTime(mapObject("updatedon"))
  val stateCodeMapper: Map[String, String] = Map(
    "Andaman and Nicobar Islands" -> AndamanAndNicobarIslands,
    "Andhra Pradesh" -> AndhraPradesh,
    "Arunachal Pradesh" -> ArunachelPradesh,
    "Assam" -> Assam,
    "Bihar" -> Bihar,
    "Chandigarh" -> Chandigarh,
    "Chhattisgarh" -> Chhattisgarh,
    "Dadra and Nagar Haveli and Daman and Diu" -> DadraAndNagarHaveli,
    "Delhi" -> Delhi,
    "Goa" -> Goa,
    "Gujarat" -> Gujarat,
    "Haryana" -> Haryana,
    "Himachal Pradesh" -> HimachalPradesh,
    "Jammu and Kashmir" -> JammuAndKashmir,
    "Jharkhand" -> Jharkhand,
    "Karnataka" -> Karnataka,
    "Kerala" -> Kerala,
    "Ladakh" -> Ladakh,
    "Madhya Pradesh" -> MadhyaPradesh,
    "Maharashtra" -> Maharashtra,
    "Manipur" -> Manipur,
    "Meghalaya" -> Meghalaya,
    "Mizoram" -> Mizoram,
    "Nagaland" -> Nagaland,
    "Odisha" -> Orissa,
    "Puducherry" -> Pondicherry,
    "Punjab" -> Punjab,
    "Rajasthan" -> Rajasthan,
    "Sikkim" -> Sikkim,
    "Tamil Nadu" -> TamilNadu,
    "Telangana" -> Telangana,
    "Tripura" -> Tripura,
    "Uttar Pradesh" -> UttarPradesh,
    "Uttarakhand" -> Uttarakhand,
    "West Bengal" -> WestBengal
  )

  val state = {
    val stateKeyFromRaw =
      mapObject.getOrElse("state", "No state key available in raw API data")
    stateCodeMapper.getOrElse(
      stateKeyFromRaw,
      "No mapped value found for the state " + stateKeyFromRaw
    )
  }

  val status: String =
    mapObject.getOrElse("tagtotaltested", "No Key found for tag")

}
