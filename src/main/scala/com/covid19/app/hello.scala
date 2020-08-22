package com.covid19

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._
import covid19.getdata
import covid19.jsonConvertor
import covid19.allStatusData
import covid19.Confirmed
import covid19.Recovered
import covid19.Deceased
import covid19.TotalTested
import java.{util => ju}
import _root_.java.io.InputStream
import java.io.FileInputStream
import covid19.stateStatus
import scala.math.pow
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.covid19.cassandra.cassandraMethods
import covid19.stateTestDaily
import com.covid19.app.RddOperations._
import covid19.Output
import covid19.BadData

object hello {

  def main(args: Array[String]) {

    // Logger.getLogger("org").setLevel(Level.ERROR)
    val log = Logger.getRootLogger()
    val inputForLog =
      (new FileInputStream("src/main/resources/log4j.properties"))
        .asInstanceOf[InputStream]
    val inputForCassandra = (new FileInputStream(
      "src/main/scala/com/covid19/app/cassandra.properties"
    )).asInstanceOf[InputStream]
    val property = new ju.Properties
    property.load(inputForLog)
    PropertyConfigurator.configure(inputForLog)
    val cassandraConf = new ju.Properties
    cassandraConf.load(inputForCassandra)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("covid19")
      .set(
        "spark.cassandra.connection.host",
        cassandraConf.getProperty("cassandra.host")
      )
      .set(
        "spark.cassandra.connection.port",
        cassandraConf.getProperty("cassandra.port")
      )

    val sc = new SparkContext(conf)
    val connector = CassandraConnector(conf)
    log.warn("Created spark context and cassandra connector")

    // Get and convert states_daily_changes from API to List of Scala Map
    val dataStatesDaily = getdata.applyVal("states_daily")

    log.warn("Received API data for states daily")

    val listOfParsedJsonStatesDaily =
      new jsonConvertor(dataStatesDaily).convert("states_daily")
      
    log.warn("Received API data for states test daily")
    // Get and convert states_test_daily from API to List of Scala Map

    val dataTestDaily = getdata.applyVal("state_test_daily")
    val listOfParsedJsonTestDaily =
      new jsonConvertor(dataTestDaily).convert("states_tested_data")
    log.warn("Converted to scala Objects")

    // Converting both List Data to RDD

    val rddTestDaily = sc.parallelize(listOfParsedJsonTestDaily, 4)
    val rddStatesDaily = sc.parallelize(listOfParsedJsonStatesDaily, 4)
    log.warn("Converted to RDD")

    //Convert to RDD of abstract DataStruct

    val dataStructTestDaily = rddTestDaily.map(mapObject => {
      new stateTestDaily(mapObject)
    })

    log.warn("Created dataStructure for States Test data")

    val dataStructStatesDaily = rddStatesDaily.map(mapObject => {
      val dataObject = new stateStatus(mapObject)
      dataObject
    })
    log.warn("Created dataStructure for States Daily")

    // Creation of allStatus for test data

    val allStatusTests = getAllStatusDataForTests(dataStructTestDaily).cache()

    val totalTested = allStatusTests.filter(data => data match {
      case TotalTested(_, _) => true
      case _ => false
    })

    log.warn("Created allStatus for  State Test Daily Data")

    // Created allStatusData for states Daily changes

    val confirmedRecoveredDeceased = dataStructStatesDaily.map(data =>
      allStatusData(data.stateInfo.toMap, data.status, data.date)
    ).cache()

    log.warn("Created allStatus for States Daily")

    val confirmed = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Confirmed(_, _) => true
          case _               => false
        }
      )

    val recovered = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Recovered(_, _) => true
          case _               => false
        }
      )

    val deceased = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Deceased(_, _) => true
          case _              => false
        }
      )

    // Gives Number of (confirmed + deceased - recovered) for each day for all states
    val effectiveIncreaseInCases = covidMap(confirmed, deceased, recovered)((x, y, z) => x + y - z)

    val effectiveIncreasePerTest = covidMap(effectiveIncreaseInCases, totalTested)((x,y) => (x/y)*1000000.toFloat)

    effectiveIncreasePerTest.take(10).foreach(x => {
      println(x.getStateValue.getOrElse("Kerala", "No key found") + " || " + x.getDate() + " || " + x.getProp())
    })
    //Data write to cassandra

    effectiveIncreaseInCases.foreachPartition(partition => {

      val session = connector.openSession()
      partition.foreach(data => {
        cassandraMethods.cassandraWrite(session, data)
      })
      session.close()
    })
    log.warn("Data write to cassandra is completed. Can cancel execution now.")

    sc.stop()

  }

}
