import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameTestUltis {
  def assertSchema(schema1:StructType, schema2:StructType)={
    val s1 = schema1.fields.map(f => (f.name,f.dataType))
    val s2 = schema2.fields.map(f => (f.name,f.dataType))
      s1.diff(s2).isEmpty
  }


  def asserData(df1:DataFrame,df2:DataFrame)={
    val data1 = df1.collect()
    val data2 = df2.collect()
    data1.diff(data2).isEmpty
  }
}
