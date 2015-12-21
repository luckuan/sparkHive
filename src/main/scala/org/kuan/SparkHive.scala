package org.kuan

import java.io.File
import java.util.Date

import com.hadoop.mapreduce.LzoTextInputFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkConf, SparkContext}

// Import Row.
import org.apache.spark.sql.Row;


/**
  * Created by Kuan on 15/12/16.
  */
object SparkHive {


  def main(args: Array[String]) {
    val Array(confPath) = args

    //配置文件
    val conf = ConfigFactory.parseFile(new File(confPath))
    //输入的文件
    val inputPath = conf.getString("inputPath")

    //schema,以,进行分隔
    val schemaString = conf.getString("schemaString")

    //hive table输出名称
    val outTable = conf.getString("outTable")

    //是否覆盖
    val overwrite = conf.getBoolean("overwrite")

    //创建sparkContext
    val sc = new SparkContext(new SparkConf())

    //创建hiveContext
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //隐式转换,将RDD转换成DataFrame
    //import hiveContext.implicits._


    //采取新的API来读取数据
    val hadoopRDD = sc.newAPIHadoopFile(inputPath, classOf[LzoTextInputFormat], classOf[LongWritable], classOf[Text])

    val schema = LineSplitter.schema(schemaString)
    //将(k,v)的key去掉,只需要用v的值
    val records = hadoopRDD.map(_._2.toString).map(LineSplitter.split(_, schema)).filter(_ != null).cache()


    val allKeys = records.map(x => {
      val time = x(0).asInstanceOf[String]
      time2String(time)
    }).distinct().collect()

    allKeys.foreach(x => {
      val rows = records.filter(r => {
        val time = r(0).asInstanceOf[String]
        time2String(time).equals(x)
      }).map(Row.fromSeq(_))

      val df = hiveContext.createDataFrame(rows, schema)
      //创建临时表
      val timeKey=x.replace(" ","").replace("-","")
      val tmpTableName = s"spark_${outTable}_${timeKey}_" + new Date().getTime
      df.registerTempTable(tmpTableName)
      //判断是否需要覆盖
      if (overwrite) {
        println(s" insert overwrite table  $outTable PARTITION (dt = '${timeKey}') select * from  $tmpTableName ")
        hiveContext.sql(s" insert overwrite table  $outTable PARTITION (dt = '${timeKey}') select * from  $tmpTableName ")
//        hiveContext.sql(s" select * from  $tmpTableName limit 10 ")
      } else {
        println(s" insert into table  $outTable PARTITION (dt = '${timeKey}') select * from  $tmpTableName ")
        hiveContext.sql(s" insert into table  $outTable PARTITION (dt = '${timeKey}') select * from  $tmpTableName ")
      }

    })
  }

  def time2String(timestamp: AnyRef): String = {
    val ret = timestamp.asInstanceOf[String]
    ret.split(":")(0)
  }
}
