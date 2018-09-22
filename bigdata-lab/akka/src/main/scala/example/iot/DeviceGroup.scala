package example.iot

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import example.iot.DeviceGroup.{ReplyDeviceList, RequestAllTemperatures, RequestDeviceList}
import example.iot.DeviceManager.RequestTrackDevice
import scala.concurrent.duration._

object DeviceGroup {
  def props(groupId: String): Props = Props(new DeviceGroup(groupId))

  final case class RequestDeviceList(requestId: Long)

  final case class ReplyDeviceList(requestId: Long, ids: Set[String])

  final case class RequestAllTemperatures(requestId: Long)

  final case class RespondAllTemperatures(requestId: Long, temperatures: Map[String, TemperatureReading])

  sealed trait TemperatureReading

  final case class Temperature(value: Double) extends TemperatureReading

  case object TemperatureNotAvailable extends TemperatureReading

  case object DeviceNotAvailable extends TemperatureReading

  case object DeviceTimedOut extends TemperatureReading

}

class DeviceGroup(groupId: String) extends Actor with ActorLogging {
  var deviceIdToActor = Map.empty[String, ActorRef]
  var actorToDeviceId = Map.empty[ActorRef, String]

  override def preStart(): Unit = log.info("DeviceGroup {} started", groupId)

  override def postStop(): Unit = log.info("DeviceGroup {} stopped", groupId)

  override def receive: Receive = {
    case trackMsg@RequestTrackDevice(`groupId`, _) ⇒
      deviceIdToActor.get(trackMsg.deviceId) match {
        case Some(deviceActor) ⇒
          deviceActor forward trackMsg
        case None ⇒
          log.info("Creating device actor for {}", trackMsg.deviceId)
          val deviceActor = context.actorOf(Device.props(groupId, trackMsg.deviceId), s"device-${trackMsg.deviceId}")

          /**
            * Akka provides a Death Watch feature that allows an actor to watch another actor and be
            * notified if the other actor is stopped. Unlike supervision, watching is not limited to
            * parent-child relationships, any actor can watch any other actor as long as it knows the
            * ActorRef. After a watched actor stops, the watcher receives a Terminated(actorRef) message
            * which also contains the reference to the watched actor. The watcher can either handle this
            * message explicitly or will fail with a DeathPactException. This latter is useful if the
            * actor can no longer perform its own duties after the watched actor has been stopped.
            * In our case, the group should still function after one device have been stopped, so we
            * need to handle the Terminated(actorRef) message.
            */
          context.watch(deviceActor)
          deviceIdToActor += trackMsg.deviceId -> deviceActor
          actorToDeviceId += deviceActor -> trackMsg.deviceId
          deviceActor forward trackMsg
      }

    case RequestTrackDevice(groupId, deviceId) ⇒
      log.warning(
        "Ignoring TrackDevice request for {}. This actor is responsible for {}.",
        groupId, this.groupId
      )

    case RequestDeviceList(requestId) ⇒
      sender() ! ReplyDeviceList(requestId, deviceIdToActor.keySet)

    case Terminated(deviceActor) ⇒
      val deviceId = actorToDeviceId(deviceActor)
      log.info("Device actor for {} has been terminated", deviceId)
      actorToDeviceId -= deviceActor
      deviceIdToActor -= deviceId

    case RequestAllTemperatures(requestId) ⇒
      context.actorOf(DeviceGroupQuery.props(
        actorToDeviceId = actorToDeviceId,
        requestId = requestId,
        requester = sender(),
        3.seconds
      ))
  }
}