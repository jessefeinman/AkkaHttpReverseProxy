package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ HttpMethods, HttpRequest, Uri }
import reverseproxy.loadbalancer.ServicesBalancer.{ HealthCheck, _ }

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.language.postfixOps
import scala.math.{ log, log10, pow }

object SingleServiceManager {
  def props(mappings: Set[Uri], failOnRedirect: Boolean, selectionCriteria: (Int, FiniteDuration) => Int = activeConnectionsCriteria)(
    implicit log: LoggingAdapter
  ): Props =
    Props(new SingleServiceManager(mappings, failOnRedirect, selectionCriteria))

  val activeConnectionsCriteria: (Int, FiniteDuration) => Int = (i: Int, _: FiniteDuration) => i
  val responseTimeCriteria: (Int, FiniteDuration)      => Int = (_: Int, d: FiniteDuration) => d.toMillis.toInt / 10

  def divDurationWithActiveConnections(millisDivisor: Int): (Int, FiniteDuration) => Int =
    (i: Int, d: FiniteDuration) => i + (d.toMillis / millisDivisor).toInt

  def logDurationWithActiveConnections(millisDivisor: Int = 10, base: Double = scala.math.E): (Int, FiniteDuration) => Int =
    base match {
      case scala.math.E =>
        (i: Int, d: FiniteDuration) =>
          i + log(d.toMillis / millisDivisor).toInt
      case 10 =>
        (i: Int, d: FiniteDuration) =>
          i + log10(d.toMillis / millisDivisor).toInt
      case num: Double =>
        (i: Int, d: FiniteDuration) =>
          i + (log10(d.toMillis / millisDivisor) / log10(num)).toInt
    }

  def powDurationWithActiveConnections(millisDivisor: Int = 10, exp: Int = 2): (Int, FiniteDuration) => Int =
    if (exp == 2) { (i: Int, d: FiniteDuration) =>
      i + { val div = d.toMillis / millisDivisor; div * div }.toInt
    } else { (i: Int, d: FiniteDuration) =>
      i + pow(d.toMillis / millisDivisor, exp).toInt
    }
}

case class Weight(weight: Int, activeConnections: Int, time: FiniteDuration) {
  override def toString: String = s"Weight(CalculatedWeight: $weight, activeConnections: $activeConnections, pingTime: $time)"
}
case class Mappings(in: Set[Uri], newWeight: (Int, FiniteDuration) => Int) {

  object SetOrdering extends Ordering[(Weight, Uri)] {
    def compare(a: (Weight, Uri), b: (Weight, Uri)): Int =
      a._1.weight - b._1.weight match {
        case 0 => if (a._2.equals(b._2)) 0 else 1
        case i => i
      }
  }

  private val weightToUri = mutable.SortedSet(in.map { case (uri: Uri) => (Weight(0, 0, 0 millis), uri) }.toSeq: _*)(SetOrdering)
  private val uriToWeight = mutable.Map(in.map { case (uri: Uri) => (uri, Weight(0, 0, 0 millis)) }.toSeq: _*)

  def state: Map[Uri, Weight] = uriToWeight.toMap
  def first: Option[Uri]      = weightToUri.headOption.map(_._2)

  private def update(uri: Uri, f: Weight => Weight): Unit =
    uriToWeight.get(uri).foreach { old =>
      val n = f(old)
      weightToUri.remove((old, uri))
      weightToUri.add((n, uri))
      uriToWeight.update(uri, n)
    }

  def fail(uri: Uri): Unit = update(uri, _.copy(weight = Int.MaxValue))

  def updateSpeed(uri: Uri, d: FiniteDuration): Unit = update(uri, old => old.copy(weight = newWeight(old.activeConnections, d), time = d))
  def updateActiveConnections(uri: Uri, modifier: Int => Int): Unit =
    update(uri, old => {
      val newConnections = {
        val newConn = modifier(old.activeConnections)
        if (newConn < 0) 0 else newConn
      }
      old.copy(weight = newWeight(newConnections, old.time), activeConnections = newConnections)
    })
}

class SingleServiceManager(inputMappings: Set[Uri], failOnRedirect: Boolean, selectionCriteria: (Int, FiniteDuration) => Int)(
  implicit log: LoggingAdapter
) extends Actor {
  private implicit val system: ActorSystem  = context.system
  private implicit val ec: ExecutionContext = context.dispatcher
  private val service                       = self.path.name

  private val mappings = Mappings(inputMappings, selectionCriteria)

  system.scheduler.schedule(0 seconds, 10 seconds, self, HealthCheck)

  log.info("Started SingleServiceManager for {} at {}", service, self.path)

  def receive: Receive = {
    case State(_)                 => sender ! mappings.state
    case Get(_)                   => sender ! getConnection
    case Succeeded(_, u)          => decrementConnection(u)
    case Failed(_, u)             => failConnection(u)
    case Unfailed(_, u)           => unfailConnection(u)
    case ConnectionSpeed(_, u, d) => mappings.updateSpeed(u, d)
    case HealthCheck              => healthCheck(service, failOnRedirect)
  }

  private def getConnection: Option[Uri] = mappings.first match {
    case Some(uri: Uri) => incrementConnection(uri); Option(uri)
    case None           => log.warning(s"No service for {} was found", service); None
  }

  private def incrementConnection(uri: Uri): Unit = mappings.updateActiveConnections(uri, (i: Int) => if (i == Int.MaxValue) Int.MaxValue else i + 1)
  private def decrementConnection(uri: Uri): Unit = mappings.updateActiveConnections(uri, (i: Int) => if (i == Int.MaxValue) 0 else i - 1)
  private def failConnection(uri: Uri): Unit      = mappings.updateActiveConnections(uri, (_: Int) => Int.MaxValue)
  private def unfailConnection(uri: Uri): Unit    = mappings.updateActiveConnections(uri, (i: Int) => if (i == Int.MaxValue) 0 else i)

  def healthCheck(service: String, failOnRedirect: Boolean)(implicit system: ActorSystem, ec: ExecutionContext): Unit =
    mappings.state.foreach {
      case (uri, weight) =>
        val start = System.nanoTime()
        Http()
          .singleRequest(HttpRequest(method = HttpMethods.GET).withUri(uri))
          .map { httpResponse =>
            val end = { (System.nanoTime() - start) nanos }
            httpResponse.status match {
              case s if s.isSuccess && weight.weight == Int.MaxValue => Seq(ConnectionSpeed(service, uri, end), Unfailed(service, uri))
              case s if failOnRedirect && s.isRedirection            => Seq(Failed(service, uri))
              case s if s.isFailure                                  => Seq(Failed(service, uri))
              case _ => Seq(ConnectionSpeed(service, uri, end))
            }
          }
          .recover { case _ => Seq(Failed(service, uri)) }
          .foreach(_.foreach(self.tell(_, self)))
    }
}
