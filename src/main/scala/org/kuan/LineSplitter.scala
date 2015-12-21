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
  def split(line: String): IndexedSeq[Any] = {
    //解析文件
    val arr = line.split( """[\]\[]""")
    val filterArray = for (i <- 0 until arr.length; if i % 2 == 1) yield {
      arr(i)
    }
    filterArray
  }

  def schema(fieldLength: Int): StructType = {
    val list = for (i <- 1 to fieldLength) yield StructField("name" + i, DataTypes.StringType, true)
    StructType(list)
  }

}
