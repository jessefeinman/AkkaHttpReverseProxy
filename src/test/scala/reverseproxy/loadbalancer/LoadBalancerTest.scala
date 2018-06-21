package reverseproxy.loadbalancer

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.model.Uri
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike, Matchers}
import reverseproxy.loadbalancer.ServicesBalancer.{Failed, Get, Succeeded}

import scala.concurrent.duration._
import scala.language.postfixOps

class LoadBalancerTest
    extends TestKit(ActorSystem("LoadBalancerTest"))
    with ImplicitSender
    with FreeSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  val max: FiniteDuration = 5 seconds
  def awaitAssert[A](a: => A): A = awaitAssert(a, max, 100 milliseconds)
  implicit val timeout: Timeout    = Timeout(max)
  implicit val log: LoggingAdapter = Logging(system.eventStream, system.name)

  override def afterAll: Unit = TestKit.shutdownActorSystem(system)

  "ServicesBalancer" - {
    val mapping = Map(
      "serviceA" -> Set(Uri().withHost("host1a"), Uri().withHost("host2a")),
      "serviceB" -> Set(Uri().withHost("host1b"), Uri().withHost("host2b"))
    )
    val testProbe        = TestProbe()
    val servicesBalancer = system.actorOf(ServicesBalancer.props(mapping), "balancer")
    "Return a URL for a service that exists" in {
      testProbe.send(servicesBalancer, Get("serviceA"))
      testProbe.fishForMessage(max) { case Some(u: Uri) if mapping("serviceA").contains(u) => true }
      testProbe.send(servicesBalancer, Get("serviceB"))
      testProbe.fishForMessage(max) { case Some(u: Uri) if mapping("serviceB").contains(u) => true }
    }
    "Return none for services that do not exist" in {
      testProbe.send(servicesBalancer, Get("notAService"))
      testProbe.fishForMessage(max) { case None => true }
    }
    "should balance" in {
      testProbe.send(servicesBalancer, Get("serviceA"))
      testProbe.send(servicesBalancer, Get("serviceA"))
      testProbe.send(servicesBalancer, Get("serviceA"))
      testProbe.send(servicesBalancer, Get("serviceA"))

      Set(testProbe.fishForSpecificMessage(max) { case Some(u: Uri) => u }, testProbe.fishForSpecificMessage(max) {
        case Some(u: Uri) => u
      }, testProbe.fishForSpecificMessage(max) { case Some(u: Uri) => u }, testProbe.fishForSpecificMessage(max) {
        case Some(u: Uri) => u
      }) should contain theSameElementsAs mapping("serviceA")
    }
    "should send the least used service when calling get Succeeded messages" in {
      testProbe.send(servicesBalancer, Succeeded("serviceA", mapping("serviceA").head))
      for (i <- Range(0, 5)) {
        testProbe.send(servicesBalancer, Succeeded("serviceA", mapping("serviceA").head))
        testProbe.send(servicesBalancer, Get("serviceA"))
        testProbe.fishForMessage(max) {
          case Some(u: Uri) if u == mapping("serviceA").head      => true
          case Some(u: Uri) if u == mapping("serviceA").tail.head => false
        }
      }
    }
    "should set failed services to the lowest priority" in {
      testProbe.send(servicesBalancer, Failed("serviceA", mapping("serviceA").head))
      for (i <- Range(0, 5)) {
        testProbe.send(servicesBalancer, Get("serviceA"))
        testProbe.fishForMessage(max) {
          case Some(u: Uri) if u == mapping("serviceA").head      => false
          case Some(u: Uri) if u == mapping("serviceA").tail.head => true
        }
      }
    }
  }
  "SingleServiceManager" - {
    "should initialize with all nodes of the given service" in {

    }
    "should track the number of active connections" in {}
    "should set failed connections to the lowest priority" in {}
    "should prioritize the least used node" in {}
  }

}
