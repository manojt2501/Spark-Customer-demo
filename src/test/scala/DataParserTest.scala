import customer_transform.{processInputRecords}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.{udf}

class DataParserTest extends AnyFunSuite with SparkSessionTestWrapper with DataFrameTestUltis {
  import spark.implicits._

 val sourceDF = Seq(
  ("cust-1","manoj","house1,stree2,city-9999888888"),
    ("cust-2","ajay","9989891234"),
    ("cust-3","friend1","house1")
  ).toDF("cust_id","cust_name","cust_contact")

//  sourceDF.show(false)
  val addAddressAndPhoneUDF = udf(getContactDetails)


  def getContactDetails: String => String = (inputContact:String) =>{
    val hasChars: String = ".*[a-zA-Z]+.*"
    inputContact match{
      case inputContact if inputContact.indexOf("-") > 0 =>
        s"${inputContact.substring(0,inputContact.indexOf("-"))};" +
          s"${inputContact.substring(inputContact.indexOf("-")+1,inputContact.length)}"
      case inputContact if inputContact.indexOf("-") < 0 && inputContact.matches(hasChars) => s"${inputContact};${null}"
      case inputContact if inputContact.indexOf("-") < 0 => s"${null};${inputContact}"
    }
  }

  val resDF = processInputRecords(sourceDF,addAddressAndPhoneUDF)

  resDF.show(false)

  val expectedDF = Seq(
    ("cust-1","manoj","house1,stree2,city","9999888888"),
    ("cust-2","ajay","null","9989891234"),
    ("cust-3","friend1","house1","null")
  ).toDF("cust_id","cust_name","Address","phone")

  expectedDF.show(false)

  assert(assertSchema(resDF.schema,expectedDF.schema))

  assert(asserData(resDF,expectedDF))

}
