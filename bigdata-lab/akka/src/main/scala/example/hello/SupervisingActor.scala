package example.hello

import akka.actor.{Actor, ActorSystem, Props}

class SupervisingActor extends Actor {
  val child = context.actorOf(Props[SupervisedActor], "supervised-actor")

  override def receive: Receive = {
    case "failChild" ⇒ child ! "fail"
  }
}

class SupervisedActor extends Actor {
  override def preStart(): Unit = println("supervised actor started")
  override def postStop(): Unit = println("supervised actor stopped")

  override def receive: Receive = {
    case "fail" ⇒
      println("supervised actor fails now")
      throw new Exception("I failed!")
  }
}

object SupervisingActor {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("test")
    val supervisingActor = system.actorOf(Props[SupervisingActor], "supervising-actor")
    supervisingActor ! "failChild"
  }
}