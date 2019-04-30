package example.remote
import java.io.File

import akka.actor.{Actor, ActorIdentity, ActorSystem, Identify, Props}
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout._
import com.typesafe.config.ConfigFactory
import org.apache.flink.runtime.messages.JobManagerMessages.RequestRunningJobsStatus

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class LocalActor extends Actor {

  import LocalActor._

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    val remoteActor =
      context.actorSelection("akka.tcp://flink@localhost:6123/user/jobmanager")
    println("That 's jobmanager:" + remoteActor)
    remoteActor ! Identify(1)
  }

  override def receive: Receive = {
    case msg: String => {
      println("got message from remote:" + msg)
    }
    case ActorIdentity(1, Some(ref)) ⇒
      println("got message from remote:" + ref)
      ref ? RequestRunningJobsStatus onComplete {
        case scala.util.Success(value) ⇒
          println(s"response $value")
        case scala.util.Failure(exception) ⇒
          println(exception)
      }
  }
}

object LocalActor {

  implicit val defaultTimeout: Timeout = 60 second

  def main(args: Array[String]): Unit = {
    val configFile =
      getClass.getClassLoader.getResource("local-application.conf").getFile
    val config = ConfigFactory.parseFile(new File(configFile))
    val system = ActorSystem("flink", config)

    val remoteActorRef = system.actorOf(props, "local")
    println(s"local actor: ${remoteActorRef.path}")
  }

  def props: Props = Props[LocalActor]
}
