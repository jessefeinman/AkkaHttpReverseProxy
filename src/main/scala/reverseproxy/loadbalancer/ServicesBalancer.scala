package reverseproxy.loadbalancer

import akka.actor.{ Actor, ActorRef, Props }
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.Uri
import reverseproxy.loadbalancer.ServicesBalancer.{ AdHoc, BalancerServiceCommand, HealthCheck, State }

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.language.postfixOps

object ServicesBalancer {
  def props(inputMappings: Map[String, Set[Uri]],
            failOnRedirect: Boolean              = false,
            healthCheckOn: Boolean               = true,
            healthCheckFrequency: FiniteDuration = 5 seconds)(implicit log: LoggingAdapter): Props =
    Props(new ServicesBalancer(inputMappings, failOnRedirect, healthCheckOn, healthCheckFrequency))

  trait BalancerCommand
  trait BalancerServiceCommand extends BalancerCommand { val service: String }

  case object HealthCheck                                                         extends BalancerCommand
  case class State(service: String)                                               extends BalancerServiceCommand
  case class AdHoc(service: String, uri: Uri)                                     extends BalancerServiceCommand
  case class Get(service: String)                                                 extends BalancerServiceCommand
  case class Succeeded(service: String, uri: Uri)                                 extends BalancerServiceCommand
  case class Failed(service: String, uri: Uri)                                    extends BalancerServiceCommand
  case class Unfailed(service: String, uri: Uri)                                  extends BalancerServiceCommand
  case class ConnectionSpeed(service: String, uri: Uri, duration: FiniteDuration) extends BalancerServiceCommand
}

class ServicesBalancer(inputMappings: Map[String, Set[Uri]], failOnRedirect: Boolean, healthCheckOn: Boolean, healthCheckFrequency: FiniteDuration)(
  implicit log: LoggingAdapter
) extends Actor {
  private implicit val ec: ExecutionContext = context.dispatcher

  if (healthCheckOn) context.system.scheduler.schedule(0 seconds, healthCheckFrequency, self, HealthCheck)

  val singleServiceManagers: mutable.Map[String, ActorRef] = mutable.Map(inputMappings.map {
    case (service, uris) => service -> context.actorOf(SingleServiceManager.props(uris, failOnRedirect), service)
  }.toSeq: _*)

  def receive: Receive = {
    case AdHoc(service, uri) =>
      if (!singleServiceManagers.contains(service)) {
        log.debug("Adding new adhoc service {} at {}", service, uri)
        singleServiceManagers.update(service, context.actorOf(SingleServiceManager.props(Set(uri), failOnRedirect), service))
      } else {
        log.debug("Request to new adhoc service {} failed due to it already being present in balancer", service, uri)
      }
    case State("balancer") => sender ! singleServiceManagers.keys
    case event: BalancerServiceCommand =>
      singleServiceManagers.get(event.service) match {
        case Some(ssm) =>
          log.debug("Forwarding request for service {}", event.service)
          ssm forward event
        case None =>
          log.debug("Requested service, {},that is not in service manager", event.service)
          sender ! None
      }
    case msg @ HealthCheck =>
      log.debug("Sending healthcheck message to all services")
      singleServiceManagers.values.foreach(_ ! msg)
  }
}
