package com.luogh.learning.lab.scala

import scalikejdbc.{ConnectionPool, DB, WrappedResultSet}
import scalikejdbc._

object ScalalikeJdbcTest extends App {

  // initialize JDBC driver & connection pool
  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://172.23.4.145:3306/cdp", "root", "")

  // ad-hoc session provider on the REPL
//  implicit val session = AutoSession

  val name = "Alice"
  // implicit session represents java.sql.Connection
  val memberId: Option[Long] = DB readOnly { implicit session =>
    sql"select id from members where name = ${name}" // don't worry, prevents SQL injection
      .map(rs => rs.long("id")) // extracts values from rich java.sql.ResultSet
      .single // single, list, traversable
      .apply() // Side effect!!! runs the SQL using Connection
  }

  println(memberId)
}

// defines entity object and extractor
import java.time._


case class Member(id: Long, name: Option[String], createdAt: ZonedDateTime)
object Member {

  def apply(rs: WrappedResultSet) = {
    rs.toMap().foreach(println _)
    new Member(
      1, rs.stringOpt("name"), rs.zonedDateTime("created_at"))
  }
}