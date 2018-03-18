package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorSystem, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpMethods, HttpRequest, Uri }
import reverseproxy.loadbalancer.ServicesBalancer.{ Failed, Get, HealthCheck, Succeeded }

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object SingleServiceManager {
  def props(mappings: Set[Uri])(implicit log: LoggingAdapter): Props = Props(new SingleServiceManager(mappings))
}

class SingleServiceManager(inputMappings: Set[Uri])(implicit log: LoggingAdapter) extends Actor {
  private implicit val system: ActorSystem  = context.system
  private implicit val ec: ExecutionContext = context.dispatcher
  private val service                       = self.path.name
  log.info("Started SingleServiceManager for {} at {}", service, self.path)
  private val mappings: mutable.SortedSet[(Int, Uri)] = mutable
    .SortedSet()(SetOrdering) ++ inputMappings.map { case (uri: Uri) => (0, uri) }
  private val hostPortLookup: mutable.Map[Uri, Int] = mutable.Map() ++ mappings.map {
    case (counter: Int, uri: Uri) => uri -> counter
  }.toMap

  def receive: Receive = {
    case Get(_)          => sender ! getConnection
    case Succeeded(_, u) => decrementConnection(u)
    case Failed(_, u)    => failConnection(u)
    case HealthCheck()   => healthCheck()
  }

  private def getConnection: Option[Uri] = Try(mappings.firstKey) match {
    case Success((_, uri)) => incrementConnection(uri); Option(uri)
    case Failure(_)        => log.warning(s"No service for {} was found", service); None
  }

  private def incrementConnection(uri: Uri): Unit =
    modify(uri, (i: Int) => if (i == Int.MaxValue) Int.MaxValue else i + 1, "increment")
  private def decrementConnection(uri: Uri): Unit =
    modify(uri, (i: Int) => if (i == Int.MaxValue) Int.MaxValue else i - 1, "decrement")
  private def failConnection(uri: Uri): Unit =
    modify(uri, (_: Int) => Int.MaxValue, "fail")
  private def unfailConnection(uri: Uri): Unit =
    modify(uri, (i: Int) => if (i == Int.MaxValue) 0 else i, "unfail")
  private def modify(uri: Uri, modifier: Int => Int, action: String): Unit = {
    val oldCounter = hostPortLookup.getOrElse(uri, 0)
    val newCounter = { val n = modifier(oldCounter); if (n < 0) 0 else n }
    (mappings.remove((oldCounter, uri)), mappings.add((newCounter, uri))) match {
      case (true, true)  => hostPortLookup.update(uri, newCounter)
      case (false, true) => log.info(s"{} is back online at {}", service, uri); hostPortLookup.update(uri, newCounter)
      case _ =>
        log.error(s"Unable to $action active connections as expected, multiple entries found for $service at $uri")
    }
  }

  private def healthCheck(): Unit =
    mappings.foreach {
      case (count, uri) =>
        Http().singleRequest(HttpRequest(method = HttpMethods.HEAD, uri = uri.withScheme("http"))).onComplete {
          case Success(httpResponse) =>
            httpResponse.status.isSuccess match {
              case ok if ok && count != Int.MaxValue => Unit
              case unfail if unfail                  => unfailConnection(uri)
              case _                                 => failConnection(uri)
            }
          case Failure(_) => failConnection(uri)
        }
    }
}

object SetOrdering extends Ordering[(Int, Uri)] {
  def compare(a: (Int, Uri), b: (Int, Uri)): Int =
    a._1 - b._1 match {
      case 0 => if (a._2.equals(b._2)) 0 else 1
      case i => i
    }
}
