package org.kuan

import java.io.File
import java.util.Date

import com.hadoop.mapreduce.LzoTextInputFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.{SparkConf, SparkContext}


import org.apache.spark.sql.Row


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
    val fieldLength = conf.getInt("fieldLength")

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

    //日志的schema
    val schema = LineSplitter.schema(fieldLength)

    //将(k,v)的key去掉,只需要用v的值
    val records = hadoopRDD.map(_._2.toString).map(LineSplitter.split).cache()

    //判断错误条数是多少
    val errorNum = records.filter(x => x.length != fieldLength).count()
    if (errorNum > 0) println(s"""***********************不符合规范的记录条数为:[$errorNum]""")

    //将不规范的记录过滤后的记录
    val filterRecords = records.filter(x => x.length == fieldLength).cache()

    //查找所有的分区key
    val allPartitionKeys = filterRecords.map(record => {
      val time = record(0).asInstanceOf[String]
      buildPartitionKey(time)
    }).distinct().collect()

    //对每个分区的key分别load到hive表中
    allPartitionKeys.foreach(partitionKey => {
      //转换成sql的row
      val rows = filterRecords.filter(record => {
        val time = record(0).asInstanceOf[String]
        buildPartitionKey(time).equals(partitionKey)
      }).map(Row.fromSeq) //转换成sql的row
      .repartition(1)//设置成一个分区,作为文件的输出,可能会增加处理时间,此处会进行shuffle处理

      //生成df
      val df = hiveContext.createDataFrame(rows, schema)
      //创建临时表
      val tmpTableName = s"spark_${outTable}_${partitionKey}_" + new Date().getTime
      df.registerTempTable(tmpTableName)
      //判断是否需要覆盖
      if (overwrite) {
        println(s""" insert overwrite table  $outTable PARTITION (dt = '$partitionKey') select * from  $tmpTableName """)
        hiveContext.sql(s""" insert overwrite table  $outTable PARTITION (dt = '$partitionKey') select * from  $tmpTableName """)
        //        hiveContext.sql(s" select * from  $tmpTableName limit 10 ")
      } else {
        println(s""" insert into table  $outTable PARTITION (dt = '$partitionKey') select * from  $tmpTableName """)
        hiveContext.sql(s""" insert into table  $outTable PARTITION (dt = '$partitionKey') select * from  $tmpTableName """)
      }
    })
  }

  def buildPartitionKey(timestamp: AnyRef): String = {
    val ret = timestamp.asInstanceOf[String]
    ret.split(":")(0).replaceAll("""\s+""","").replaceAll("-", "")
  }
}
