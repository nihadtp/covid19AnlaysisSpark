package com.covid19.cassandra

import covid19.allStatusData
import java.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.datastax.oss.driver.api.core.servererrors._
import org.apache.log4j.Logger
import com.datastax.oss.driver.api.core.DriverException
import com.datastax.oss.driver.api.core.NoNodeAvailableException
import com.datastax.oss.driver.api.core.CqlSession

class cassandraMethods(session: CqlSession) extends Serializable {

  private val log = Logger.getRootLogger()
  private val keySpace = "covid19"
  private val stateTable = "state_data"
  private val countryTable = "country_stat"
  private val formatter = DateTimeFormat.forPattern("YYYY-MM-dd")
  private val exception = (e: Exception) =>
    e match {
      case e: QueryValidationException =>
        log.fatal("Wrong query. Message => " + e.getMessage())
      case e: WriteTimeoutException =>
        log.fatal("Write Time out Exception exception => " + e.getMessage())
      case e: NoNodeAvailableException =>
        log.fatal("Could not connect to cassandra host" + e.getMessage())
      case _: Throwable => log.warn("Cassandra Error")
    }

  def cassandraWriteForStateData(data: allStatusData): Unit = {
    val stateCode = data.getStateValue
    val dateTime = (data.dateValue).toString(formatter)
    val stateProp = data.getProp()

    try {
      stateCode.foreach(kv => {
        val state_code = kv._1
        val state_value = kv._2

        // log.warn(
        //   "Writing %s state code with value %s on date %s"
        //     .format(state_code, state_value, dateTime)
        // )

        session.execute(
          """INSERT INTO "%s".%s ( state_code, state_value, date, property )
            VALUES ('%s', %s, '%s', '%s');"""
            .format(
              keySpace,
              stateTable,
              state_code,
              state_value,
              dateTime,
              stateProp
            )
        )

      })
    } catch {
      case e: Exception => exception(e)
    }
  }

  def cassandraWriteForCountryStat(data: allStatusData): Unit = {
    val dateTime = (data.dateValue).toString(formatter)
    val stateProp = data.getProp()
    val maxVal = data.maxValue
    val minVal = data.minValue
    val maxValStates = data.maxValueStates.foldLeft("")((x, y) => x + " | " + y)
    val minValStates = data.minValueStates.foldLeft("")((x, y) => x + " | " + y)
    try {
      session.execute(
        """INSERT INTO "%s".%s (state_prop, country_prop, date, states, value ) 
            VALUES ('%s', 'maxVal', '%s', '%s', %s);
      """.format(
          keySpace,
          countryTable,
          stateProp,
          dateTime,
          maxValStates,
          maxVal
        )
      )

      session.execute(
        """INSERT INTO "%s".%s (state_prop, country_prop, date, states, value ) VALUES ('%s', 'minVal', '%s', '%s', %s);
      """.format(
          keySpace,
          countryTable,
          stateProp,
          dateTime,
          minValStates,
          minVal
        )
      )
    } catch {
      case e: Exception => exception(e)
    }
  }
}
