package com.luogh.learning.lab.flink

import org.apache.flink.api.scala.utils._
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object ZipWithIndexTest {

  def main(args: Array[String]): Unit = {

    test(10)
    test1(10)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val set = env.fromCollection(1 to 100).setParallelism(10)
    set.zipWithUniqueId.setParallelism(10).print()


  }


  def test(parallel: Int): Unit = {
    import org.apache.flink.api.java.utils.DataSetUtils

    val maxBitSize: Long = DataSetUtils.getBitSize(Long.MaxValue)

    val shifter = DataSetUtils.getBitSize(parallel - 1)

    for (i <- 0 until parallel) {
      println(s"----${i}----")
      var start: Long = 0
      var label: Long = 0
      for (j <- 0 until parallel) {
        label = (start << shifter) + i
        if (DataSetUtils.getBitSize(start) + shifter < maxBitSize) {
          println(label -> (i * parallel + j))
          start += 1
        }
      }
      println(s"---${i} end---")
    }
  }

  def test1(parallel: Int): Unit = {
    for (i <- 0 until parallel) {
      println(s"----${i}----")
      var start: Long = 0
      var label: Long = 0
      for (j <- 0 until parallel) {
        label = parallel * start + i
        println(label -> (i * parallel + j))
        start += 1
      }
      println(s"---${i} end---")
    }
  }
}
