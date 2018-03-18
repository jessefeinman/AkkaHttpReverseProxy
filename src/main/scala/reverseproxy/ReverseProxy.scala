package reverseproxy

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{HttpApp, Route}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.Config
import reverseproxy.loadbalancer.ServicesBalancer

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

class ReverseProxy(config: Config) extends HttpApp {
  implicit val timeout: Timeout     = Timeout(5 seconds)
  implicit val system: ActorSystem  = ActorSystem("reverseProxy", config)
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter  = Logging(system.eventStream, "Route")

  def parseServerMappings(config: Config): Map[String, Set[Uri]] =
    config
      .getConfigList("services")
      .asScala
      .map { c =>
        c.getString("service") ->
        c.getConfigList("nodes")
          .asScala
          .map { n =>
            Uri()
              .withHost(n.getString("host"))
              .withPort(n.getInt("port"))
          }
          .toSet
      }
      .toMap

  val balancer: ActorRef = system.actorOf(ServicesBalancer.props(parseServerMappings(config)), "Balancer")
  private def getUri(service: String): Future[Option[Uri]] =
    (balancer ? ServicesBalancer.Get(service)).mapTo[Option[Uri]]
  private def succeeded(service: String, uri: Uri): Unit = balancer ! ServicesBalancer.Succeeded(service, uri)
  private def failed(service: String, uri: Uri): Unit    = balancer ! ServicesBalancer.Failed(service, uri)

  val serviceNotInMappings =
    HttpResponse(
      status = StatusCodes.ServiceUnavailable,
      entity = HttpEntity("Request service is not in service to server mappings")
    )
  val serviceUnavailable =
    HttpResponse(status = StatusCodes.RequestTimeout, entity = HttpEntity("Request to service timed out"))

  def routes: Route = get {
    extract(_.request) { request =>
      onComplete(requestToService(request)) {
        case Success(response) =>
          complete(response)
        case Failure(e) =>
          log.error(e, "Failure when contacting service")
          complete(serviceUnavailable)
      }
    }
  }

  private def requestToService(request: HttpRequest): Future[HttpResponse] = {
    val service = request.uri.path.tail.head.toString
    val path    = request.uri.path.tail.tail
    getUri(service).flatMap {
      case Some(uri) =>
        Http.apply
          .singleRequest(
            request.withUri(
              request.uri
                .withHost(uri.authority.host)
                .withPort(uri.authority.port)
                .withPath(path)
            )
          )
          .map(m => { succeeded(service, uri); m })
          .recover {
            case t =>
              log.warning(t.getMessage)
              failed(service, uri)
              serviceUnavailable
          }

      case None => Future(serviceNotInMappings)
    }
  }
}
