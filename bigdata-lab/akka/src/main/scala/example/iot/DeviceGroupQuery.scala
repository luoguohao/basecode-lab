package example.iot

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}

import scala.concurrent.duration.FiniteDuration

object DeviceGroupQuery {

  case object CollectionTimeout

  def props(
             actorToDeviceId: Map[ActorRef, String],
             requestId: Long,
             requester: ActorRef,
             timeout: FiniteDuration
           ): Props = {
    Props(new DeviceGroupQuery(actorToDeviceId, requestId, requester, timeout))
  }
}

class DeviceGroupQuery(
                        actorToDeviceId: Map[ActorRef, String],
                        requestId: Long,
                        requester: ActorRef,
                        timeout: FiniteDuration
                      ) extends Actor with ActorLogging {

  import DeviceGroupQuery._
  import context.dispatcher

  val queryTimeoutTimer = context.system.scheduler.scheduleOnce(timeout, self, CollectionTimeout)

  override def preStart(): Unit = {
    actorToDeviceId.keysIterator.foreach { deviceActor ⇒
      context.watch(deviceActor)
      deviceActor ! Device.ReadTemperature(0)
    }
  }

  override def postStop(): Unit = {
    queryTimeoutTimer.cancel()
  }

  override def receive: Receive =
    waitingForReplies(
      Map.empty,
      actorToDeviceId.keySet
    )

  def waitingForReplies(
                         repliesSoFar: Map[String, DeviceGroup.TemperatureReading],
                         stillWaiting: Set[ActorRef]
                       ): Receive = {
    case Device.RespondTemperature(0, valueOption) ⇒
      log.info("received ...")
      val deviceActor = sender()
      val reading = valueOption match {
        case Some(value) ⇒ DeviceGroup.Temperature(value)
        case None ⇒ DeviceGroup.TemperatureNotAvailable
      }
      receivedResponse(deviceActor, reading, stillWaiting, repliesSoFar)

    case Terminated(deviceActor) ⇒
      receivedResponse(deviceActor, DeviceGroup.DeviceNotAvailable, stillWaiting, repliesSoFar)

    case CollectionTimeout ⇒
      val timedOutReplies =
        stillWaiting.map { deviceActor ⇒
          val deviceId = actorToDeviceId(deviceActor)
          deviceId -> DeviceGroup.DeviceTimedOut
        }
      log.info("operator timeout")
      requester ! DeviceGroup.RespondAllTemperatures(requestId, repliesSoFar ++ timedOutReplies)
      context.stop(self)
  }

  /**
    * The query actor, apart from the pending timer, has one stateful aspect, tracking the set of
    * actors that: have replied, have stopped, or have not replied. One way to track this state
    * is to create a mutable field in the actor (a var). A different approach takes advantage of
    * the ability to change how an actor responds to messages. A Receive is just a function
    * (or an object, if you like) that can be returned from another function. By default, the receive
    * block defines the behavior of the actor, but it is possible to change it multiple times during
    * the life of the actor. We call context.become(newBehavior) where newBehavior is anything with
    * type Receive (which is a shorthand for PartialFunction[Any, Unit]). We will leverage this feature
    * to track the state of our actor.
    *
    * @param deviceActor
    * @param reading
    * @param stillWaiting
    * @param repliesSoFar
    */
  def receivedResponse(
                        deviceActor:  ActorRef,
                        reading:      DeviceGroup.TemperatureReading,
                        stillWaiting: Set[ActorRef],
                        repliesSoFar: Map[String, DeviceGroup.TemperatureReading]
                      ): Unit = {

    /**
      * e treated Terminated as the implicit response DeviceNotAvailable, so receivedResponse does
      * not need to do anything special. However, there is one small task we still need to do. It
      * is possible that we receive a proper response from a device actor, but then it stops during
      * the lifetime of the query. We don’t want this second event to overwrite the already received
      * reply. In other words, we don’t want to receive Terminated after we recorded the response.
      * This is simple to achieve by calling context.unwatch(ref). This method also ensures that we
      * don’t receive Terminated events that are already in the mailbox of the actor. It is also
      * safe to call this multiple times, only the first call will have any effect, the rest is ignored.
      */
    context.unwatch(deviceActor)
    val deviceId = actorToDeviceId(deviceActor)
    val newStillWaiting = stillWaiting - deviceActor

    val newRepliesSoFar = repliesSoFar + (deviceId -> reading)
    if (newStillWaiting.isEmpty) { // all actor has query completed, stop the query actor and response the requester
      requester ! DeviceGroup.RespondAllTemperatures(requestId, newRepliesSoFar)
      context.stop(self)
    } else {
      // change the actor query receive operator
      context.become(waitingForReplies(newRepliesSoFar, newStillWaiting))
    }
  }

}