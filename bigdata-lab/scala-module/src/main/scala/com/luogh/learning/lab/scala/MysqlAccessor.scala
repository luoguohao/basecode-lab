package com.luogh.learning.lab.scala

import java.sql.Connection
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.logging.log4j.scala.Logging
import scalikejdbc.{ConnectionPool, _}
import scalikejdbc.config.DBs

object MysqlAccessor extends Logging {

  @transient private val isInit: AtomicBoolean = new AtomicBoolean(false)

  if (!isInit.get()) {
    DBs.setupAll()
    this.isInit.set(true)
    logger.info(s"init mysql connection pool with dbs: ${DBs.dbNames.mkString(",")}")
  }

  def executeInConnection[T](operator: Connection => T): T = {
    require(this.isInit.get(), "MysqlAccessor is not initialize yet")
    using(ConnectionPool.borrow()) { conn =>
      operator(conn)
    }
  }

  def close(): Unit = {
    if (this.isInit.get()) {
      DBs.closeAll()
      this.isInit.set(false)
    }
  }
}
