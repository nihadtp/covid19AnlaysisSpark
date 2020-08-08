package com.covid19

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._
import covid19.getdata
import covid19.jsonConvertor
import covid19.dataStructure
import org.joda.time.DateTime
import  covid19.allStatusData
import covid19.Confirmed
import covid19.Recovered
import covid19.Deceased
import covid19.Operations._
import covid19.BadData
import java.{util => ju}
import _root_.java.io.InputStream
import java.io.FileInputStream
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import covid19.stateStatus
import scala.math.pow
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector



object hello {

    def covidMap(rdd1: RDD[allStatusData], 
        rdd2: RDD[allStatusData], 
        rdd3: RDD[allStatusData])(f: (Float, Float, Float) => Float): RDD[allStatusData] = {
            
            val cartesian = rdd1.cartesian(rdd2).cartesian(rdd3).filter(triplet => {
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
    
    val format = DateTimeFormat.forPattern("dd-MMM-YY")
    
    def main(args: Array[String]) {

       // Logger.getLogger("org").setLevel(Level.ERROR)
        val log = Logger.getRootLogger()
        val inputForLog = (new FileInputStream("src/main/resources/log4j.properties")).asInstanceOf[InputStream]
        val inputForCassandra = (new FileInputStream("src/main/scala/com/covid19/app/cassandra.properties")).asInstanceOf[InputStream]
        val property = new ju.Properties
        property.load(inputForLog)
        PropertyConfigurator.configure(inputForLog)
        val cassandraConf = new ju.Properties
        cassandraConf.load(inputForCassandra)

        log.warn(cassandraConf.getProperty("cassandra.host") )


        val conf = new SparkConf()  
            .setMaster("local[*]")
            .setAppName("covid19")
            .set("spark.cassandra.connection.host", cassandraConf.getProperty("cassandra.host"))
           
        val sc = new SparkContext(conf)
        
        val session = CassandraConnector(conf)
        
        val data = getdata.applyVal()
        val listOfParsedJson = new jsonConvertor(data).convert()

        val rdd = sc.parallelize(listOfParsedJson, 4)
        val dataStruct = rdd.map(mapObject => {
            val dataObject = new stateStatus(mapObject)
            dataObject
        })
        log.warn("Created dataStruc1")

        val confirmedRecoveredDeceased = dataStruct.map(data => allStatusData(data.stateInfo.toMap, data.status, data.date)) 
        
        val confirmed = confirmedRecoveredDeceased.filter( data => data match {
            case Confirmed(_, _) => true
            case _ => false
        }).cache()

        val recovered = confirmedRecoveredDeceased.filter( data => data match {
            case Recovered(_, _) => true
            case _ => false
        }).cache()

        val deceased = confirmedRecoveredDeceased.filter( data => data match {
            case Deceased(_, _) => true
            case _ => false
        }).cache()

        // Gives Number of (confirmed - recovered - deceased) for each day for all states hi testing
        val delta = covidMap(confirmed, recovered, deceased)((x, y, z) => x - y - z)

        //Gives (confirmed^2 - recovered^2 - deceased^2 / confirmed^2 + recovered^2 + deceased^2) for all states

        val delta_square  = covidMap(confirmed, recovered, deceased)((x, y, z) => {
            val f = x.toDouble; val s = y.toDouble; val t = z.toDouble;
            (pow(f, 2) - pow(s, 2) - pow(t, 2)).toFloat/(math.sqrt(pow(f, 2) + pow(s, 2) + pow(t, 2))).toFloat
        })

        
        delta.take(100).foreach(data => println(data.maxValueStates + ", " + data.getProp() + ", " + data.dateValue.toString(format)))
        sc.stop()
       
    }

    
}
