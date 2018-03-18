package reverseproxy.loadbalancer

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.http.scaladsl.model.Uri
import akka.testkit.{ ImplicitSender, TestKit }
import akka.util.Timeout
import org.scalatest.{ BeforeAndAfterAll, FreeSpecLike, Matchers }

import scala.concurrent.duration._
import scala.language.postfixOps

class LoadBalancerTest
    extends TestKit(ActorSystem("LoadBalancerTest"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll {

  def awaitAssert[A](a: => A): A = awaitAssert(a, 3 seconds, 100 milliseconds)
  implicit val timeout: Timeout    = Timeout(3 seconds)
  implicit val log: LoggingAdapter = Logging(system.eventStream, system.name)

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  "ServicesBalancer" - {
    val mapping = Map(
      "serviceA" -> Set(Uri().withHost("host1a"), Uri().withHost("host2a")),
      "serviceB" -> Set(Uri().withHost("host1b"), Uri().withHost("host2b"))
    )
    val servicesBalancer = system.actorOf(ServicesBalancer.props(mapping), "balancer")

    "should create an actor for each service" in {}
    "should forward BalancerEvents to the correct actor" in {}
    "should send HealthCheck messages all actors" in {}
  }
  "SingleServiceManager" - {
    "should initialize with all nodes of the given service" in {}
    "should track the number of active connections" in {}
    "should set failed connections to the lowest priority" in {}
    "should prioritize the least used node" in {}
  }

}
