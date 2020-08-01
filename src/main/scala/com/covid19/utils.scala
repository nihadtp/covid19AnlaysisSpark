package com.covid19

import org.joda.time.DateTime

object utils {
  def getDateList(dateInput: DateTime): List[DateTime] = {
      var date = dateInput
      var listOfDates = scala.collection.mutable.ListBuffer[DateTime]()
      while(!date.isAfter(new DateTime())){
      listOfDates += date
      date = date.plusDays(1)
      }
      listOfDates.toList
  }
}
