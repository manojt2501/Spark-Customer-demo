import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf, split}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object customer_transform extends App {

  val spark = SparkSession.builder()
    .appName("customer_info")
    .master("local[*]")
    .getOrCreate()

  val customerSchema = StructType(List(StructField("cust_id",StringType),
    StructField("cust_name",StringType),StructField("cust_contact",StringType)
  ))

  val customerContactDF = spark.read
    .schema(customerSchema)
    .format("csv")
    .option("header", true)
    .load("D:\\med_item_hdr\\cust_contact.csv")

  val customerImpressionDF = spark.read
    .option("inferSchema",true)
    .format("csv")
    .option("header",true)
    .load("D:\\med_item_hdr\\cust_impression.csv")

  val productDF = spark.read
    .option("inferSchema",true)
    .format("csv")
    .option("header",true)
    .load("D:\\med_item_hdr\\product_id.csv")

  val addAddressAndPhoneUDF = udf(getContactDetails)
  val customerDF = processInputRecords(customerContactDF,addAddressAndPhoneUDF)
  customerDF.show(false)

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

  def processInputRecords(sourceDF:DataFrame, contactFunction: UserDefinedFunction):DataFrame = {
    sourceDF
      .withColumn("AddressAndPhone",contactFunction(col("cust_contact")))
      .withColumn("Address",split(col("AddressAndPhone"),";").getItem(0))
      .withColumn("phone",split(col("AddressAndPhone"),";").getItem(1))
      .drop("AddressAndPhone")
  }

  def joinOnKey(df1:DataFrame)(joinOnKey:String)(df2:DataFrame)={
    df2.join(df1,Seq(joinOnKey),"left")
  }

  def joinOnKey2(df1:DataFrame,joinOnKey:String,df2:DataFrame)={
    df2.join(df1,Seq(joinOnKey),"left")
  }

  def getCustomerContact:DataFrame=> DataFrame = joinOnKey(customerDF)("cust_id")(_)

  getCustomerContact(customerImpressionDF).show(false)

  val getProductInfo = joinOnKey(customerImpressionDF)("product_id")(productDF)

  getProductInfo.show(false)

  def getCustomerContactCurried = (joinOnKey2 _).curried

  def C1 = getCustomerContactCurried(customerImpressionDF)("product_id")(_)

  C1(productDF).show(false)

}
