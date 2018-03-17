package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorRef, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.Uri
import reverseproxy.loadbalancer.ServicesBalancer.{ BalancerEvents, HealthCheck }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.language.postfixOps

object ServicesBalancer {
  def props(serviceMappings: Map[String, Set[Uri]])(implicit log: LoggingAdapter): Props =
    Props(new ServicesBalancer(serviceMappings))

  trait BalancerEvents { val service: String }
  case class Get(service: String)                extends BalancerEvents
  case class Succeeded(service: String, uri: Uri) extends BalancerEvents
  case class Failed(service: String, uri: Uri)   extends BalancerEvents
  case class HealthCheck()

}

class ServicesBalancer(inputMappings: Map[String, Set[Uri]], healthCheckFrequency: FiniteDuration = 30 seconds)(
  implicit log: LoggingAdapter
) extends Actor {
  private implicit val ec: ExecutionContext = context.dispatcher

  context.system.scheduler.schedule(0 seconds, healthCheckFrequency, self, HealthCheck())

  val singleServiceManagers: Map[String, ActorRef] = inputMappings.map {
    case (service, uris) => service -> context.system.actorOf(SingleServiceManager.props(uris), service)
  }

  def receive: Receive = {
    case event: BalancerEvents =>
      singleServiceManagers.get(event.service) match {
        case Some(ssm) => ssm forward event
        case None      => sender ! None
      }
    case msg: HealthCheck => singleServiceManagers.values.foreach(_ ! msg)
  }
}