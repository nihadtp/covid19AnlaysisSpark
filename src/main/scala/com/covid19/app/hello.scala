package com.covid19

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._
import covid19.getdata
import covid19.jsonConvertor
import covid19.dataStructure
import covid19.allStatusData
import covid19.Confirmed
import covid19.Recovered
import covid19.Deceased
import covid19.Operations._
import covid19.BadData
import java.{util => ju}
import _root_.java.io.InputStream
import java.io.FileInputStream
import covid19.stateStatus
import scala.math.pow
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.covid19.cassandra.cassandraMethods
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions
import com.datastax.oss.driver.shaded.netty.handler.ssl.ApplicationProtocolConfig.Protocol


object hello {

  def covidMap(
      rdd1: RDD[allStatusData],
      rdd2: RDD[allStatusData],
      rdd3: RDD[allStatusData]
  )(f: (Float, Float, Float) => Float): RDD[allStatusData] = {

    /* Takes in 3 rdd of allStatus data get triple product and filter those having 
    data available for same date*/
    val cartesian = rdd1
      .cartesian(rdd2)
      .cartesian(rdd3)
      .filter(triplet => {
        val operand1 = triplet._1._1
        val operand2 = triplet._1._2
        val operand3 = triplet._2
        val hasSameDate = operand1.dateValue.isEqual(operand2.dateValue) &&
          operand2.dateValue.isEqual(operand3.dateValue)
        hasSameDate
      })

    cartesian.map(data => {
      val status1 = data._1._1
      val status2 = data._1._2
      val status3 = data._2

      operate(status1, status2, status3)(f)
    })

  }


  def sparkConf(arg: String): SparkConf = {
    arg match {
      case "local" => {
        val inputForCassandra = (new FileInputStream(
      "src/main/scala/com/covid19/app/cassandra.properties"
          )).asInstanceOf[InputStream]
        val cassandraConf = new ju.Properties
        cassandraConf.load(inputForCassandra)
        new SparkConf()
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
      }

      case "aws" => {
         new SparkConf()
      .setMaster("local[*]")
      .setAppName("covid19")
      // .set("spark.cassandra.connection.host" ,"cassandra.us-east-2.amazonaws.com")
      // .set("spark.cassandra.connection.port", "9142")
      // .set("spark.cassandra.auth.username", "nihadTp-at-898924162355")
      // .set("spark.cassandra.auth.password", "simOjKszIzuDLvmy3EBdXjloYt/FzOoh1gviqVTaz4I=")
      // .set("spark.cassandra.connection.ssl.enabled", "true")
      // .set("spark.cassandra.connection.ssl.trustStore.path", "/home/nihad/.cassandra/cassandra_truststore.jks")
      // .set("spark.cassandra.connection.ssl.trustStore.password", "indiana_jones")
      // .set("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
      
      }
    }
  }

  def createSession(arg: String): CqlSession = {
    arg match {
      case "local" => {
        val conf = sparkConf(arg)
        val connector = CassandraConnector(conf)
        
        connector.openSession()
      }

      case "aws" => {
        val conf = sparkConf(arg)
        val connector = CassandraConnector(conf)
        
        //connector.openSession()
        val loader = DriverConfigLoader.fromClasspath("application.conf")
        CqlSession.builder().withConfigLoader(loader).build()     
      }
      
    }
  }

  def main(args: Array[String]) {

    // Logger.getLogger("org").setLevel(Level.ERROR)
    val log = Logger.getRootLogger()
    val inputForLog =
      (new FileInputStream("src/main/resources/log4j.properties"))
        .asInstanceOf[InputStream]
     
    
    val property = new ju.Properties
    property.load(inputForLog)
    PropertyConfigurator.configure(inputForLog)
    

    val conf = sparkConf(args(0))
    val sc = new SparkContext(conf)
    val data = getdata.applyVal()
    val listOfParsedJson = new jsonConvertor(data).convert()

    val rdd = sc.parallelize(listOfParsedJson, 4)
    val dataStruct = rdd.map(mapObject => {
      val dataObject = new stateStatus(mapObject)
      dataObject
    })
    log.warn("Created dataStructure")

    val confirmedRecoveredDeceased = dataStruct.map(data =>
      allStatusData(data.stateInfo.toMap, data.status, data.date)
    )

    val confirmed = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Confirmed(_, _) => true
          case _               => false
        }
      )
      .cache()

    val recovered = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Recovered(_, _) => true
          case _               => false
        }
      )
      .cache()

    val deceased = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Deceased(_, _) => true
          case _              => false
        }
      )
      .cache()

    // Gives Number of (confirmed - recovered - deceased) for each day for all states 
    val delta = covidMap(confirmed, recovered, deceased)((x, y, z) => x - y - z)

    //Gives (confirmed^2 - recovered^2 - deceased^2 / confirmed^2 + recovered^2 + deceased^2) for all states

    val delta_square = covidMap(confirmed, recovered, deceased)((x, y, z) => {
      val f = x.toDouble; val s = y.toDouble; val t = z.toDouble;
      (pow(f, 2) - pow(s, 2) - pow(t, 2)).toFloat / (math
        .sqrt(pow(f, 2) + pow(s, 2) + pow(t, 2)))
        .toFloat
    })
     
    delta.foreachPartition(partition => {

      val session = createSession(args(0))
      partition.foreach(data => {
        cassandraMethods.cassandraWrite(session, data)
      })
      session.close()
    })
    
    log.warn("Data write to cassandra is completed. Can cancel execution now.")

    sc.stop()

  }

}
