package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorRef, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.Uri
import reverseproxy.loadbalancer.ServicesBalancer.{ BalancerServiceCommand, HealthCheck, State }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.language.postfixOps

object ServicesBalancer {
  def props(serviceMappings: Map[String, Set[Uri]])(implicit log: LoggingAdapter): Props =
    Props(new ServicesBalancer(serviceMappings))

  trait BalancerCommand
  trait BalancerServiceCommand extends BalancerCommand { val service: String }

  case object HealthCheck                                                         extends BalancerCommand
  case class State(service: String)                                               extends BalancerServiceCommand
  case class Get(service: String)                                                 extends BalancerServiceCommand
  case class Succeeded(service: String, uri: Uri)                                 extends BalancerServiceCommand
  case class Failed(service: String, uri: Uri)                                    extends BalancerServiceCommand
  case class Unfailed(service: String, uri: Uri)                                    extends BalancerServiceCommand
  case class ConnectionSpeed(service: String, uri: Uri, duration: FiniteDuration) extends BalancerServiceCommand
}

class ServicesBalancer(inputMappings: Map[String, Set[Uri]],
                       failOnRedirect: Boolean              = true,
                       healthCheckOn: Boolean               = true,
                       healthCheckFrequency: FiniteDuration = 30 seconds)(implicit log: LoggingAdapter)
    extends Actor {
  private implicit val ec: ExecutionContext = context.dispatcher

  if (healthCheckOn) context.system.scheduler.schedule(0 seconds, healthCheckFrequency, self, HealthCheck)

  val singleServiceManagers: Map[String, ActorRef] = inputMappings.map {
    case (service, uris) => service -> context.actorOf(SingleServiceManager.props(uris, failOnRedirect), service)
  }

  def receive: Receive = {
    case State("balancer") => sender ! singleServiceManagers.mapValues(_.path)
    case event: BalancerServiceCommand =>
      singleServiceManagers.get(event.service) match {
        case Some(ssm) => ssm forward event
        case None      => sender ! None
      }
    case msg@HealthCheck => singleServiceManagers.values.foreach(_ ! msg)
  }
}
