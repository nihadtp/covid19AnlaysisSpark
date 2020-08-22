package covid19
import org.apache.log4j._
import com.covid19.dataStructures.stateNameConstants._

trait Operations[A] {

  protected final val stateCodeMapConstant = Map(
    AndamanAndNicobarIslands -> Float.NaN,
    AndhraPradesh -> Float.NaN,
    ArunachelPradesh -> Float.NaN,
    Assam -> Float.NaN,
    Bihar -> Float.NaN,
    Chhattisgarh -> Float.NaN,
    Chandigarh -> Float.NaN,
    DadraAndNagarHaveli -> Float.NaN,
    DamanAndDiu -> Float.NaN,
    Delhi -> Float.NaN,
    Goa -> Float.NaN,
    Gujarat -> Float.NaN,
    Haryana -> Float.NaN,
    HimachalPradesh -> Float.NaN,
    JammuAndKashmir -> Float.NaN,
    Jharkhand -> Float.NaN,
    Karnataka -> Float.NaN,
    Kerala -> Float.NaN,
    Ladakh -> Float.NaN,
    Lakshadweep -> Float.NaN,
    MadhyaPradesh -> Float.NaN,
    Maharashtra -> Float.NaN,
    Manipur -> Float.NaN,
    Meghalaya -> Float.NaN,
    Mizoram -> Float.NaN,
    Nagaland -> Float.NaN,
    Orissa -> Float.NaN,
    Pondicherry -> Float.NaN,
    Punjab -> Float.NaN,
    Rajasthan -> Float.NaN,
    Sikkim -> Float.NaN,
    TamilNadu -> Float.NaN,
    Telangana -> Float.NaN,
    Tripura -> Float.NaN,
    UttarPradesh -> Float.NaN,
    UttarPradesh -> Float.NaN,
    WestBengal -> Float.NaN
  )

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

        // Extracting State Map Values from all 3 allStatus data
        val op1StateCodeMap = op1.getStateValue
        val op2StateCodeMap = op2.getStateValue
        val op3StateCodeMap = op3.getStateValue
        val stateValue = stateCodeMapConstant.map(stateCodeValue => {

          val stateCode = stateCodeValue._1
          // Extracting value from individual states
          val stateVal1 = op1StateCodeMap.get(stateCode)
          val stateVal2 = op2StateCodeMap.get(stateCode)
          val stateVal3 = op3StateCodeMap.get(stateCode)

          val result = (stateVal1, stateVal2, stateVal3) match {
            case (Some(a), Some(b), Some(c)) =>
              f(a.toFloat, b.toFloat, c.toFloat)
            case (_, _, _) => {
              Float.NaN
            }
          }
          (stateCode -> result.toFloat)
        })
        new Output(stateValue, "operate3", op1.dateValue)
      }

      /* This method is same as above but for binary operation instead of tertiary */
      def operate(op1: allStatusData, op2: allStatusData)(
          f: (Float, Float) => Float
      ): allStatusData = {
        val op1StateCodeMap = op1.getStateValue
        val op2StateCodeMap = op2.getStateValue
        val stateValue = stateCodeMapConstant.map(stateCodeValue => {
          val stateCode = stateCodeValue._1
          val stateVal1 = op1StateCodeMap.get(stateCode)
          val stateVal2 = op2StateCodeMap.get(stateCode)

          val result = (stateVal1, stateVal2) match {
            case (Some(a), Some(b)) => f(a.toFloat, b.toFloat)
            case (_, _) => {
              //log.fatal("No value found for State key " + stateCodeValue._1 + " on " + op1.getDate() + " and " + op2.getDate())
              Float.NaN
            }
          }
          (stateCode -> result.toFloat)
        })
        new Output(stateValue, "operate2", op1.dateValue)
      }
    }
}
