package covid19
import org.apache.log4j._

trait Operations[A] {
  def operate(op1: A, op2: A, op3: A)(f:(Float, Float, Float) => Float): A
}

object Operations{
  val log = Logger.getRootLogger()
  
  def operate[A](op1: A, op2: A, op3: A)(f:(Float, Float, Float) => Float)(implicit d: Operations[A]) = d.operate(op1, op2, op3)(f)

  implicit lazy val operateForAllStatusData: Operations[allStatusData] = new Operations[allStatusData] {

    def operate(op1: allStatusData, op2: allStatusData, op3: allStatusData)(f:(Float, Float, Float) => Float): allStatusData = {
      var isValidKey = true
      val op1StateCodeMap = op1.getStateValue
      val op2StateCodeMap = op2.getStateValue
      val op3StateCodeMap = op3.getStateValue
      val stateValue = op1StateCodeMap.map( stateCodeValue => {
        val stateVal1 = op1StateCodeMap.get(stateCodeValue._1)
        val stateVal2 = op2StateCodeMap.get(stateCodeValue._1)
        val stateVal3 = op3StateCodeMap.get(stateCodeValue._1)
        //log.warn(stateVal1 + ", " + stateVal2 + ", " + stateVal3 + ", " + stateCodeValue._1 + ", " + op1.getDate() + op2.getDate()+ op3.getDate())

        val result = (stateVal1, stateVal2, stateVal3) match {
          case (Some(a), Some(b), Some(c)) => f(a.toFloat, b.toFloat, c.toFloat)
          case (_, _, _) => isValidKey = false ; 0.0
        }
        (stateCodeValue._1 -> result.toFloat)
      })
      if (isValidKey)
      new Output(stateValue, "operate", op1.dateValue)
      else
      new BadData(stateValue,"BadKey", op1.dateValue)
    }    

  }
}

