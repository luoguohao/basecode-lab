package com.luogh.learning.lab.flink

sealed trait TriggerType extends Serializable

case class SessionStartTrigger(triggerTimestamp: Long) extends TriggerType
case class SessionEndTrigger(triggerTimestamp: Long) extends TriggerType