package com.luogh.learning.lab.flink

import java.io.IOException
import java.util.concurrent.{ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import grizzled.slf4j.Logging
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.util.{Failure, Success, Try}

class HbaseOutputFormat(tableName: String) extends OutputFormat[(String, Long)] with Logging {

  var conn: Connection = _
  var table: Table = _
  var hbaseConfig: org.apache.hadoop.conf.Configuration = _

  import HbaseOutputFormat._

  override def configure(configuration: Configuration): Unit = {
    logger.debug(s"output form at configuration:${configuration}")
    hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.setInt("hbase.client.scanner.timeout.period", 6000000)
  }

  override def writeRecord(it: (String, Long)): Unit = {

    val rowKey = it._1 match {
      case r if r.isEmpty => EMPTY_ROW_KEY
      case _ => it._1
    }
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("count"), Bytes.toBytes(it._2))
    table.put(put)
  }

  @throws[IOException]
  override def close(): Unit = {
    table.close()
    conn.close()
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    logger.info(s"taskNumber:${taskNumber}, numTasks:${numTasks}")
    Try(ConnectionFactory.createConnection(hbaseConfig, executorService)) match {
      case Success(conn) =>
        this.conn = conn
        this.table = conn.getTable(TableName.valueOf(tableName))
      case Failure(ex) =>
        throw new RuntimeException("create Hbase Connection failed.", ex)
    }
  }
}

object HbaseOutputFormat {

  val executorService: ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat("hbase-client-%d").build()
    Executors.newCachedThreadPool(threadFactory)
  }

  def apply(tableName: String, conf: Configuration): HbaseOutputFormat = {
    if (tableName.isEmpty) throw new RuntimeException(s"invalid table name ${tableName}")
    new HbaseOutputFormat(tableName)
  }
}
