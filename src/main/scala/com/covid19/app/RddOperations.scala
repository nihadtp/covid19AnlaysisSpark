package com.covid19.app

import org.apache.spark.rdd.RDD
import covid19.stateTestDaily
import covid19.allStatusData
import covid19.Operations._

object RddOperations {

  // Operation to Convert Type stateTestDaily to allStatusData

  def getAllStatusDataForTests(
      statesTestsDaily: RDD[stateTestDaily]
  ): RDD[allStatusData] = {
    statesTestsDaily
      .groupBy(_.date)
      .flatMapValues(statesDaily => {

        var negativeAllStatesMap = scala.collection.mutable.Map[String, Float]()
        var positiveAllStatesMap = scala.collection.mutable.Map[String, Float]()
        var populationAllStatesMap =
          scala.collection.mutable.Map[String, Float]()
        var inQuarantineAllStatesMap =
          scala.collection.mutable.Map[String, Float]()
        var releasedFromQuarantineAllStatesMap =
          scala.collection.mutable.Map[String, Float]()
        var totalTestedAllStatesMap =
          scala.collection.mutable.Map[String, Float]()

        val date = statesDaily.head.date
        statesDaily.foreach(data => {

          val state = data.state
          val negative = data.stateInfo.getOrElse("negative", Float.NaN)
          val positive = data.stateInfo.getOrElse("positive", Float.NaN)
          val population = data.stateInfo.getOrElse("population", Float.NaN)
          val inQuarantine = data.stateInfo.getOrElse("inQuarantine", Float.NaN)
          val releasedFromQuarantine =
            data.stateInfo.getOrElse("releasedFromQuarantine", Float.NaN)
          val totalTested = data.stateInfo.getOrElse("totalTested", Float.NaN)

          negativeAllStatesMap += (state -> negative)
          positiveAllStatesMap += (state -> positive)
          populationAllStatesMap += (state -> population)
          inQuarantineAllStatesMap += (state -> inQuarantine)
          releasedFromQuarantineAllStatesMap += (state -> releasedFromQuarantine)
          totalTestedAllStatesMap += (state -> totalTested)
        })

        List(
          allStatusData(negativeAllStatesMap.toMap, "Negative", date),
          allStatusData(positiveAllStatesMap.toMap, "Positive", date),
          allStatusData(populationAllStatesMap.toMap, "Population", date),
          allStatusData(inQuarantineAllStatesMap.toMap, "InQuarantine", date),
          allStatusData(
            releasedFromQuarantineAllStatesMap.toMap,
            "ReleasedFromQuarantine",
            date
          ),
          allStatusData(totalTestedAllStatesMap.toMap, "TotalTested", date)
        )
      })
      .map(_._2)

  }

  // Tertiary Operation for allStatusData type
  def covidMap(
      rdd1: RDD[allStatusData],
      rdd2: RDD[allStatusData],
      prop: String
  )(f: (Float, Float) => Float): RDD[allStatusData] = {

    /* Takes in 3 rdd of allStatus data get triple product and filter those having
    data available for same date*/
    val cartesian = rdd1
      .cartesian(rdd2)
      .filter(triplet => {
        val operand1 = triplet._1
        val operand2 = triplet._2
        val hasSameDate = operand1.dateValue.isEqual(operand2.dateValue) 
        hasSameDate
      })

    cartesian.map(data => {
      val status1 = data._1
      val status2 = data._2
      //println(status1.getDate() + " for " + status1.getProp() + " AND " + status2.getDate() + " for " + status2.getProp())
      operate(status1, status2, prop)(f)
    })

  }

  def covidMap(
      rdd1: RDD[allStatusData],
      rdd2: RDD[allStatusData],
      rdd3: RDD[allStatusData],
      prop: String
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

      operate(status1, status2, status3, prop)(f)
    })

  }

}
