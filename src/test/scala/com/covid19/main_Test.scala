import org.scalatest.funsuite.AnyFunSuite
import covid19.jsonConvertor
import com.covid19.inputOutput
import covid19.dataStructure
import covid19.allStatusData
import covid19.stateStatus
import covid19.Confirmed
import org.joda.time.DateTime
import covid19.Recovered
import covid19.Deceased

class main_Test extends AnyFunSuite {
    val testData = inputOutput.testData
    val correctOutput = inputOutput.output

  test("Check proper JSON Conversion") {
    val output = new jsonConvertor(testData).convert()
    assert(output === correctOutput)
  }

    val dataStruct = correctOutput.map(data => new stateStatus(data))
    val caseClassed = dataStruct.map(data => allStatusData(data.stateInfo.toMap, data.status, data.date))
  test("Check dataStructure to case class conversion") {
    
    assert(caseClassed(0).isInstanceOf[Confirmed])
    assert(caseClassed(1).isInstanceOf[Recovered])
    assert(caseClassed(2).isInstanceOf[Deceased])
  }
  test("Testing max value of property on a day") {
    
    assert(caseClassed(0).maxValue === 19.0)
  }
  
  test("Testing for getting list of stat code for the highest value for a property") {
    assert(caseClassed(0).maxValueStates === List("kl"))
  }
}
