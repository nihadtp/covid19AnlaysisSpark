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
import covid19.stateStatus
import covid19.stateTestDaily
import com.covid19.app.RddOperations._
import covid19.Output
import com.datastax.oss.driver.api.core.CqlSession
import com.covid19.utils._

object hello {

  def main(args: Array[String]) {

    // Initialized Apache Log configuration
    log_init()
    val log = Logger.getRootLogger()

    log.warn("Initialized Apache Log configuration and log variable")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("covid19")
    val sc = new SparkContext(conf)

    log.warn("Created spark context")

    // Get data from state daily and state test data from API
    val dataStatesDaily = getdata.getDataFromAPI("states_daily")
    
    val dataTestDaily = getdata.getDataFromAPI("state_test_daily")

    log.warn("Received API data for states daily changes and state test daily ")

    // Convert API json data state daily and test daily to scala objects
    val listOfParsedJsonStatesDaily =
      new jsonConvertor(dataStatesDaily).convert("states_daily")
    val listOfParsedJsonTestDaily =
      new jsonConvertor(dataTestDaily).convert("states_tested_data")

    log.warn("Converted state daily and test daily API data to  Scala objects")
    // Converting both List Data to RDD

    val rddTestDaily = sc.parallelize(listOfParsedJsonTestDaily, 12)
    val rddStatesDaily = sc.parallelize(listOfParsedJsonStatesDaily, 12)
    log.warn("Converted Scala objects to to RDD")

    //Convert data from RDD of scala Object to RDD of Map objects

    val dataStructTestDaily = rddTestDaily.map(mapObject => {
      new stateTestDaily(mapObject)
    })

    val dataStructStatesDaily = rddStatesDaily.map(mapObject => {
      val dataObject = new stateStatus(mapObject)
      dataObject
    })

    log.warn("Convert state daily and test daily to RDD of Maps")

    // Convert RDD of states daily and tests daily to a unified data structure RDD of allStatus
    val allStatusTests = getAllStatusDataForTests(dataStructTestDaily).cache()
    val confirmedRecoveredDeceased = dataStructStatesDaily
      .map(data => allStatusData(data.stateInfo.toMap, data.status, data.date))
      .cache()

    log.warn(
      "Converted RDD of states daily and states tests daily to a unified data structure. RDD of allStatus"
    )

    // Extracting Confirmed data for individual states for individual day till current day
    val confirmed = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Confirmed(_, _) => true
          case _               => false
        }
      )

    // Extracting Recovered data for individual states for individual day till current day
    val recovered = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Recovered(_, _) => true
          case _               => false
        }
      )

    // Extracting Deceased data for individual states for individual day till current day
    val deceased = confirmedRecoveredDeceased
      .filter(data =>
        data match {
          case Deceased(_, _) => true
          case _              => false
        }
      )

    // Extracting Number of people tested data for individual states for individual day till current day
    val totalTested = allStatusTests.filter(data =>
      data match {
        case TotalTested(_, _) => true
        case _                 => false
      }
    )

    log.warn(
      "Extracted confirmed, recovered, deceased and total number of people tested data for all states till current day"
    )

    // Calculated effective increase in cases per day [(confirmed cases + deceased - recovered)]
    val effectiveIncreaseInCases =
      covidMap(confirmed, deceased, recovered, "effectiveIncrease")((x, y, z) => x + y - z)

    log.warn(
      "Calculated effective increase in cases per day (Confirmed + Deceased - Recovered)"
    )

    // Calculated effectiveIncrease in cases per Million tests [(effectiveIncreaseInCases / totalTested)*1000000]
    val effectiveIncreasePerMillionTests =
      covidMap(effectiveIncreaseInCases, totalTested, "effectiveIncreasePerMillionTest")((x, y) =>
        (x / y) * 1000000.toFloat
      )

    log.warn(
      "Calculated effective increase in cases per million tests per day (effectiveIncreaseInCases / totalTested)*1000000"
    )

    log.warn(
      "Started writing results to cassandra for effective increase in cases"
    )
       
   
    writeToStateTable(effectiveIncreaseInCases, args)

    log.warn(
      "Started writing results to cassandra for effective increase in cases per Million"
    )

    writeToStateTable(effectiveIncreasePerMillionTests, args)

    log.warn(
      "Started writing results to cassandra country stat table for Confirmed cases"
    )

    writeToCountryTable(confirmed, args)

   log.warn(
      "Started writing results to cassandra country stat table for Deceased cases"
    )


    writeToCountryTable(recovered, args)

    log.warn(
      "Started writing results to cassandra country stat table for Recovered cases"
    )

    

   writeToCountryTable(deceased, args)

   log.warn("Started writing results to cassandra country stat table for Effective Increase cases")

   writeToCountryTable(effectiveIncreaseInCases, args)
   
    log.warn(
      "Data write to cassandra is completed and cassandra session is closed"
    )

    sc.stop()

  }

  
}
