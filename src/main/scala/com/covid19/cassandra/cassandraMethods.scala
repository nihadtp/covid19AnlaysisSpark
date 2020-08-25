package com.covid19.cassandra

import covid19.allStatusData
import java.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.datastax.oss.driver.api.core.servererrors._
import org.apache.log4j.Logger
import com.datastax.spark.connector.CassandraSparkExtensions
import com.datastax.oss.driver.api.core.DriverException
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.CqlSession


object cassandraMethods {

  val log = Logger.getRootLogger()
  val keySpace = "covid19"
  val table = "delta_by_states"
  val formatter = DateTimeFormat.forPattern("YYYY-MM-dd")

  def cassandraWrite(session: CqlSession, data: allStatusData): Unit = {
    val stateCode = data.getStateValue
    val dateTime = (data.dateValue).toString(formatter)

    try {
      stateCode.foreach(kv => {
        val state_code = kv._1
        val state_value = kv._2
        
        // log.warn(
        //   "Writing %s state code with value %s on date %s"
        //     .format(state_code, state_value, dateTime)
        // )
        
        session.execute(
          """INSERT INTO "%s".%s ( state_code, state_value, date )
            VALUES ('%s', %s, '%s');"""
            .format(keySpace, table, state_code, state_value, dateTime)
        )

      })
    } catch {
      case e: QueryValidationException =>
        log.fatal("Wrong query. Message => " + e.getMessage())
      case e: WriteTimeoutException =>
        log.fatal("Write Time out Exception exception => " + e.getMessage())
      case e: NoNodeAvailableException =>
        log.fatal("Could not connect to cassandra host" + e.getMessage())
      case _: Throwable => log.warn("Cassandra Error")
    }
  }
}
