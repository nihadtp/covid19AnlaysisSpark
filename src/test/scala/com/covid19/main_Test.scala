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

  test("Check dataStructure to case class conversion") {
    val dataStruct = correctOutput.map(data => new stateStatus(data))
    val caseClassed = dataStruct.map(data => allStatusData(data.stateInfo.toMap, data.status, data.date)) 
    assert(caseClassed(0).isInstanceOf[Confirmed])
    assert(caseClassed(1).isInstanceOf[Recovered])
    assert(caseClassed(2).isInstanceOf[Deceased])
  }
  
  
}
