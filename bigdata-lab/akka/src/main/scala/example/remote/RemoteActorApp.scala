package example.remote
import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object RemoteActorApp {

  def main(args: Array[String]): Unit = {
    val configFile =
      getClass.getClassLoader.getResource("remote-application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    val system = ActorSystem("flink", config)

    val remoteActorRef = system.actorOf(RemoteActor.props, "jobmanager")
    println(s"remote actor: ${remoteActorRef.path}")
  }
}
