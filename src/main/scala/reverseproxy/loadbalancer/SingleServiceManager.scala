package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorSystem, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpMethods, HttpRequest, Uri }
import reverseproxy.loadbalancer.ServicesBalancer._

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object SingleServiceManager {
  def props(mappings: Set[Uri], failOnRedirect: Boolean)(implicit log: LoggingAdapter): Props =
    Props(new SingleServiceManager(mappings, failOnRedirect))
}

class SingleServiceManager(inputMappings: Set[Uri], failOnRedirect: Boolean)(implicit log: LoggingAdapter) extends Actor {
  private implicit val system: ActorSystem  = context.system
  private implicit val ec: ExecutionContext = context.dispatcher
  private val service                       = self.path.name

  private val mappings: mutable.SortedSet[(Int, Uri)] = mutable.SortedSet()(SetOrdering) ++ inputMappings.map { case (uri: Uri) => (0, uri) }
  private val hostPortLookup: mutable.Map[Uri, Int]   = mutable.Map() ++ mappings.map { case (counter: Int, uri: Uri) => uri -> counter }.toMap

  log.info("Started SingleServiceManager for {} at {}", service, self.path)

  def receive: Receive = {
    case State(_)        => sender ! (mappings, hostPortLookup)
    case Get(_)          => sender ! getConnection
    case Succeeded(_, u) => decrementConnection(u)
    case Failed(_, u)    => failConnection(u)
    case HealthCheck()   => healthCheck()
  }

  private def getConnection: Option[Uri] = Try(mappings.firstKey) match {
    case Success((_, uri)) => incrementConnection(uri); Option(uri)
    case Failure(_)        => log.warning(s"No service for {} was found", service); None
  }

  sealed trait Action
  case object Increment extends Action
  case object Decrement extends Action
  case object Fail      extends Action
  case object Unfail    extends Action

  private def incrementConnection(uri: Uri): Unit = modify(uri, (i: Int) => if (i == Int.MaxValue) Int.MaxValue else i + 1, Increment)
  private def decrementConnection(uri: Uri): Unit = modify(uri, (i: Int) => if (i == Int.MaxValue) 0 else i - 1, Decrement)
  private def failConnection(uri: Uri): Unit      = modify(uri, (_: Int) => Int.MaxValue, Fail)
  private def unfailConnection(uri: Uri): Unit    = modify(uri, (i: Int) => if (i == Int.MaxValue) 0 else i, Unfail)

  private def modify(uri: Uri, modifier: Int => Int, action: Action): Unit = {
    val oldCounter = hostPortLookup.getOrElse(uri, 0)
    val newCounter = {
      val n = modifier(oldCounter);
      if (n < 0) 0 else n
    }
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
            httpResponse.status match {
              case s if s.isSuccess && count == Int.MaxValue => unfailConnection(uri)
              case s if failOnRedirect && s.isRedirection    => failConnection(uri)
              case s if s.isFailure                          => failConnection(uri)
              case _ => Unit
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
