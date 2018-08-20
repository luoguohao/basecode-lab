package com.luogh.learning.lab.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object WordCountBatchApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.fromElements("To be, or not to be,--that is the question:--",
      "Whether 'tis nobler in the mind to suffer",
      "The slings and arrows of outrageous fortune",
      "Or to take arms against a sea of troubles,")

    dataSet.flatMap(x => x.split("\\W+"))
      .map(x => (x, 1))
      .groupBy(0)
      .sum(1).print()
  }
}
