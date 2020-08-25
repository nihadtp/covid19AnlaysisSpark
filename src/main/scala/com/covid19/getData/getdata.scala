package covid19

import org.apache.log4j.Logger

object getdata {
  val log = Logger.getRootLogger()
  

  private val urlStatesDaily = "https://api.covid19india.org/states_daily.json"
  private val urlTestDaily = "https://api.covid19india.org/state_test_data.json"

  def getDataFromAPI(urlString: String): String = {

    import java.net.{URL, HttpURLConnection}
    val url = urlString match {
      case "states_daily" => urlStatesDaily
      case "state_test_daily" => urlTestDaily
    } 

    try {
      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(5000)
    connection.setReadTimeout(5000)
    connection.setRequestMethod("GET")
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  } catch {
    case ioe: java.io.IOException =>  log.fatal("API request failed. I/O Exception" + ioe.getMessage()); ""
    case ste: java.net.SocketTimeoutException => log.fatal("API request failed. Timeout Exception" + ste.getMessage()); "" 
  }

  }

  

}
