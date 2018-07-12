package reverseproxy

import akka.actor.{ ActorRef, ActorSystem }
import akka.event.{ Logging, LoggingAdapter }
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{ HttpApp, Route }
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.Config
import reverseproxy.loadbalancer.{ ServicesBalancer, Weight }

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

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
          .map(
            n =>
              Uri()
                .withHost(n.getString("host"))
                .withScheme(n.getString("protocol"))
                .withPort(n.getInt("port"))
          )
          .toSet
      }
      .toMap

  val balancer: ActorRef = system.actorOf(ServicesBalancer.props(parseServerMappings(config)), "balancer")

  private def getState(service: String): Future[Map[Uri, Weight]] = (balancer ? ServicesBalancer.State(service)).mapTo[Map[Uri, Weight]]
  private def getStateOfBalancer: Future[Iterable[String]]        = (balancer ? ServicesBalancer.State("balancer")).mapTo[Iterable[String]]
  private def getUri(service: String): Future[Option[Uri]]        = (balancer ? ServicesBalancer.Get(service)).mapTo[Option[Uri]]
  private def succeeded(service: String, uri: Uri): Unit          = balancer ! ServicesBalancer.Succeeded(service, uri)
  private def failed(service: String, uri: Uri): Unit             = balancer ! ServicesBalancer.Failed(service, uri)

  val serviceNotInMappings =
    HttpResponse(status = StatusCodes.ServiceUnavailable, entity = HttpEntity("Request service is not in service to server mappings"))
  val serviceUnavailable =
    HttpResponse(status = StatusCodes.RequestTimeout, entity = HttpEntity("Request to service timed out"))

  def routes: Route = get {
    pathPrefix("adHocService") {
      extract(_.request) { request =>
        newAdHocService(request) match {
          case Success(_) =>
            complete("Added new service")
          case Failure(e) =>
            val msg = s"Unable to add ad-hoc service due to ${e.getMessage}"
            log.info(msg)
            complete(HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(msg)))
        }
      }
    } ~
    pathPrefix("currentState") {
      extract(_.request) { request =>
        onComplete(queryState(request)) {
          case Success(response) =>
            complete(response)
          case Failure(e) =>
            log.error(e, "Failure when contacting service")
            complete(serviceUnavailable)
        }
      }
    } ~
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

  private def newAdHocService(request: HttpRequest): Try[Unit] = Try {
    val uriString       = request.uri.path.tail.tail.tail.tail.toString()
    val portRegex       = ":\\d+".r
    val portRegex(port) = uriString
    val uri = Uri()
      .withHost(uriString.replaceFirst(":\\d+", ""))
      .withPort(port.tail.toInt)
    val service = request.uri.path.tail.tail.tail.head.toString
    balancer ! ServicesBalancer.AdHoc(service, uri)
  }

  private def queryState(request: HttpRequest): Future[String] =
    request.uri.path.tail.tail.tail.head.toString match {
      case "balancer" =>
        getStateOfBalancer.map { sob =>
          Future.sequence(sob.map { svc =>
            getState(svc).map((svc, _))
          })
        }.flatten.map(_.toMap.toString())
      case service: String =>
        getState(service).map(_.toString())
    }

  private def requestToService(request: HttpRequest): Future[HttpResponse] = {
    val service = request.uri.path.tail.head.toString
    val path    = request.uri.path.tail.tail
    getUri(service).flatMap {
      case Some(uri) =>
        Http.apply
          .singleRequest(request.withUri(request.uri.withHost(uri.authority.host).withPort(uri.authority.port).withPath(path)))
          .map { r =>
            succeeded(service, uri); r
          }
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
