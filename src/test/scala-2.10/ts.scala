import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.kuan.LineSplitter

import scala.util.parsing.json.{JSON, JSONObject}

/**
  * Created by Kuan on 15/12/20.
  */
object obj {
  def main(args: Array[String]) {
    /**
    val s =
      """
        |[{
        |	"name":"name1",
        |	"dataType":"string",
        |	"nullable":true
        |},{
        |	"name":"name2",
        |	"dataType":"int",
        |	"nullable":false
        |}]
      """.stripMargin
**/

//   val schema =  LineSplitter.schema(s)
    /**
    val json = JSON.parseFull(s)
    val row: StructType = json match {
      case Some(list: List[Map[String, Any]]) => {
        StructType(list.map(x => {
          val name = x.get("name").get.asInstanceOf[String]
          val dataTypeStr = x.get("dataType").get.asInstanceOf[String]
          val nullable = x.get("nullable").get.asInstanceOf[Boolean]
          val dataType = dataTypeStr match {
            case "string" => DataTypes.StringType
            case "boolean" => DataTypes.BooleanType
            case "date" => DataTypes.DateType
            case "timestamp" => DataTypes.TimestampType
            case "long" => DataTypes.LongType
            case "int" => DataTypes.IntegerType
            case "float" => DataTypes.FloatType
            case "double" => DataTypes.DoubleType
            case other => throw new RuntimeException(s"未知的数据类型${other}")
          }
          StructField(name, dataType, nullable)
        }))
      }
      case None => null
    }
      **/
   // print(row.find(x=>x.name.equals("name1")))
//    print(row.fieldIndex("name3"))

    /***
    val ss = "ab,cd"
    val rows = Row.fromSeq(ss.split(","))
    println(rows)

    val row = Row("ab", "cd".trim)

    print(row.toString())


**/
    val s=
      """
        |[{
        |	"name":"name1",
        |	"dataType":"string",
        |	"nullable":true
        |},{
        |	"name":"name2",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name3",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name4",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name5",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name6",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name7",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name8",
        |	"dataType":"long",
        |	"nullable":false
        |},{
        |	"name":"name9",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name10",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name11",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name12",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name13",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name14",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name15",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name16",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name17",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name18",
        |	"dataType":"int",
        |	"nullable":false
        |},{
        |	"name":"name19",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name20",
        |	"dataType":"string",
        |	"nullable":false
        |},{
        |	"name":"name21",
        |	"dataType":"string",
        |	"nullable":false
        |}]
      """.stripMargin

    print(LineSplitter.schema(s))
//    println(s)
  }
}
