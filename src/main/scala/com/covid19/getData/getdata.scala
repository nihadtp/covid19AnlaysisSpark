package covid19

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import org.apache.log4j.Logger

object getdata {
  val log = Logger.getRootLogger()
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  private val urlStatesDaily = "https://api.covid19india.org/states_daily.json"
  private val urlTestDaily = "https://api.covid19india.org/state_test_data.json"

  def getResponse(urlString: String): Future[String] = {

    val url = urlString match {
      case "states_daily" => urlStatesDaily
      case "state_test_daily" => urlTestDaily
    }
    val responseFuture: Future[HttpResponse] =
      Http().singleRequest(HttpRequest(uri = url))
    val entityFuture: Future[HttpEntity.Strict] =
      responseFuture.flatMap(response => response.entity.toStrict(10.seconds))
    entityFuture.map(entity => entity.data.utf8String)
  }

  def applyVal(data: String): String = {
    var output: String = ""
    getResponse(data).onComplete {

      case Success(res) => output += res
      case Failure(_)   => {
        log.fatal("Didn't get response from API for " + data)}
        new Exception("No data from API")
    }
    Thread.sleep(9000)
    output
  }

}
