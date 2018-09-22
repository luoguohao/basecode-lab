package com.luogh.learning.lab.scala

import java.io.{File, FileReader}
import java.util.Properties

import scalikejdbc.mapper.ScalikejdbcCodeGenerator

object ScalikeJdbcReverseEngineerTest extends App {

  val srcDir = new File("/Users/luogh/Code_Repository/talkingdata/cdp-parent/cdp-realtime/src/main/scala/td/enterprise/dmp/realtime/")
  val testDir = new File("/Users/luogh/Code_Repository/talkingdata/cdp-parent/cdp-realtime/src/test/scala/td/enterprise/dmp/realtime/service")

  val properties = new Properties()
  properties.load(new FileReader(new File("/Users/luogh/Code_Repository/talkingdata/cdp-parent/cdp-realtime/src/main/resources/scalikejdbc.properties")))

  val tables = Seq("biz_trait_trigger", "biz_trait_strategy", "biz_trait_condition", "biz_trait", "biz_trait_action", "biz_trait_config_value", "biz_trait_optional_value")
  ScalikejdbcCodeGenerator.generatorWithTable(tables, srcDir, testDir, properties)

}
