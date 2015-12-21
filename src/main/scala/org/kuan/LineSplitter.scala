package org.kuan

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.immutable.IndexedSeq
import scala.util.parsing.json.JSON

/**
  * Created by Kuan on 15/12/20.
  */
object LineSplitter {
  def split(line: String, schema: StructType): IndexedSeq[Any] = {
    //解析文件
    val arr = line.split( """[\]\[]""")
    val filterArray = for (i <- 0 until arr.length; if i % 2 == 1) yield {
      arr(i)
    }
    if (filterArray.size != schema.size) {
      null
    } else {
      filterArray
    }


    /**
    filterArray.zipWithIndex.map(x => {
      val index = x._2
      val dataType = schema(index).dataType
      dataType match {
        case StringType => x._1.asInstanceOf[String]
        case BooleanType => x._1.asInstanceOf[Boolean]
        case DateType => x._1.asInstanceOf[Date]
        case TimestampType => x._1.asInstanceOf[Timestamp]
        case LongType => x._1
        case IntegerType => {

          val str = x._1
          str
        }
        case FloatType => x._1.asInstanceOf[Float]
        case DoubleType => x._1.asInstanceOf[Double]
        case other => throw new RuntimeException(s"未知的数据类型${other}")
      }
    }
    )
      * */
    //    Row.fromSeq(list)
  }

  def schema(schemaLength: Int): StructType = {
    val list = for (i <- 1 to schemaLength) yield StructField("name" + i, DataTypes.StringType, true)
    StructType(list)

    //    println(schemaString)
    //    val json = JSON.parseFull(schemaString)
    //    println(json)
    /**
    json match {
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
            StructField(name, DataTypes.StringType, nullable)
          }))
        }
        case None => throw new RuntimeException(s"未能正确解析[${schemaString}]")
      }
      * */
  }

}
