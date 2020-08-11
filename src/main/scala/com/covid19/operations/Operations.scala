package covid19
import org.apache.log4j._

trait Operations[A] {
  def operate(op1: A, op2: A, op3: A)(f: (Float, Float, Float) => Float): A
  def operate(op1: A, op2: A)(f: (Float, Float) => Float): A
}

object Operations {
  val log = Logger.getRootLogger()

  // Implicit declaration for 3 parameter allStatus Type method
  def operate[A](op1: A, op2: A, op3: A)(f: (Float, Float, Float) => Float)(
      implicit d: Operations[A]
  ) = d.operate(op1, op2, op3)(f)

  // Implicit declaration for 2 parameter allStatus type method
  def operate[A](op1: A, op2: A)(f: (Float, Float) => Float)(implicit
      d: Operations[A]
  ) = d.operate(op1, op2)(f)

  // Method Implementation
  implicit lazy val operateForAllStatusData: Operations[allStatusData] =
    new Operations[allStatusData] {

      /* Method takes in 3 allStatus data and a 3 argument function as parameter.
         Function is intended to do mathematical operation between 3 values for same state
         on same date.
       */
      def operate(op1: allStatusData, op2: allStatusData, op3: allStatusData)(
          f: (Float, Float, Float) => Float
      ): allStatusData = {
        var isValidKey = true

        // Extracting State Map Values from all 3 allStatus data
        val op1StateCodeMap = op1.getStateValue
        val op2StateCodeMap = op2.getStateValue
        val op3StateCodeMap = op3.getStateValue
        val stateValue = op1StateCodeMap.map(stateCodeValue => {

          // Extracting value from individual states
          val stateVal1 = op1StateCodeMap.get(stateCodeValue._1)
          val stateVal2 = op2StateCodeMap.get(stateCodeValue._1)
          val stateVal3 = op3StateCodeMap.get(stateCodeValue._1)

          val result = (stateVal1, stateVal2, stateVal3) match {
            case (Some(a), Some(b), Some(c)) =>
              f(a.toFloat, b.toFloat, c.toFloat)
            case (_, _, _) => {
              isValidKey = false;
              log.fatal("No value found for State key " + stateCodeValue._1)
              0.0
            }
          }
          (stateCodeValue._1 -> result.toFloat)
        })
        if (isValidKey)
          new Output(stateValue, "operate3", op1.dateValue)
        else
          new BadData(stateValue, "BadKey", op1.dateValue)
      }

      /* This method is same as above but for binary operation instead of tertiary */
      def operate(op1: allStatusData, op2: allStatusData)(
          f: (Float, Float) => Float
      ): allStatusData = {
        var isValidKey = true
        val op1StateCodeMap = op1.getStateValue
        val op2StateCodeMap = op2.getStateValue
        val stateValue = op1StateCodeMap.map(stateCodeValue => {
          val stateVal1 = op1StateCodeMap.get(stateCodeValue._1)
          val stateVal2 = op2StateCodeMap.get(stateCodeValue._1)

          val result = (stateVal1, stateVal2) match {
            case (Some(a), Some(b)) => f(a.toFloat, b.toFloat)
            case (_, _) => {
              isValidKey = false;
              log.fatal("No value found for State key " + stateCodeValue._1)
              0.0
            }
          }
          (stateCodeValue._1 -> result.toFloat)
        })
        if (isValidKey)
          new Output(stateValue, "operate", op1.dateValue)
        else
          new BadData(stateValue, "BadKey", op1.dateValue)
      }
    }
}
