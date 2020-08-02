package covid19

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
 

object getdata {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val url = "https://api.covid19india.org/states_daily.json"

    def getResponse(): Future[String] = {
        val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))
        val entityFuture: Future[HttpEntity.Strict] = responseFuture.flatMap(response => response.entity.toStrict(5.seconds))
        entityFuture.map(entity => entity.data.utf8String)
    }
     
    def applyVal(): String = {
        var output: String = ""
        getResponse.onComplete {
        case Success(res) => output += res
        case Failure(_)   => println("something wrong")
      }  
      Thread.sleep(5000)  
      output
    }   

}