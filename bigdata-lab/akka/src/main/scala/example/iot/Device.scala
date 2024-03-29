package example.iot

import akka.actor.{Actor, ActorLogging, Props}

object Device {
  def props(groupId: String, deviceId: String): Props = Props(new Device(groupId, deviceId))

  final case class RecordTemperature(requestId: Long, value: Double)

  final case class TemperatureRecorded(requestId: Long)

  final case class ReadTemperature(requestId: Long)

  final case class RespondTemperature(requestId: Long, value: Option[Double])


}

class Device(groupId: String, deviceId: String) extends Actor with ActorLogging {

  import Device._

  var lastTemperatureReading: Option[Double] = None

  override def preStart(): Unit = log.info("Device actor {}-{} started", groupId, deviceId)

  override def postStop(): Unit = log.info("Device actor {}-{} stopped", groupId, deviceId)

  override def receive: Receive = {
    //We used a feature of scala pattern matching where we can check to see if a certain field equals
    // an expected value. By bracketing variables with backticks, like `variable`, the pattern will
    // only match if it contains the value of variable in that position.
    case DeviceManager.RequestTrackDevice(`groupId`, `deviceId`) ⇒
      sender() ! DeviceManager.DeviceRegistered

    case DeviceManager.RequestTrackDevice(groupId, deviceId) ⇒
      log.warning(
        "Ignoring TrackDevice request for {}-{}.This actor is responsible for {}-{}.",
        groupId, deviceId, this.groupId, this.deviceId
      )

    case RecordTemperature(id, value) ⇒
      log.info("Recorded temperature reading {} with {}", value, id)
      lastTemperatureReading = Some(value)
      sender() ! TemperatureRecorded(id)

    case ReadTemperature(id) ⇒
      sender() ! RespondTemperature(id, lastTemperatureReading)
  }
}