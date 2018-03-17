package reverseproxy

import com.typesafe.config.ConfigFactory

object Main extends App {
  val reverseProxyService = new ReverseProxy(ConfigFactory.load)
  reverseProxyService.startServer("localhost", port = 8080)
}
