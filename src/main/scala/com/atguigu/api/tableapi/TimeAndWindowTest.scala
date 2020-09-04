package com.atguigu.api.tableapi

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.tableapi
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/4 15:38
  */
object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = StreamTableEnvironment.create(env)

    // 读取数据
//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 将流转换成Table，同时设定时间字段

    // 1. 处理时间
//    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts, 'pt.proctime)

    // 2. 事件时间
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    // 3. 窗口聚合
    // 3.1 分组窗口

    // 3.1.1 Table API
    val resultTable = sensorTable
      .window( Tumble over 10.seconds on 'ts as 'tw )
      .groupBy( 'id, 'tw )
      .select( 'id, 'id.count as 'cnt, 'tw.end )

    // 3.1.2 SQL实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, count(id) as cnt, tumble_end(ts, interval '10' second)
        |from sensor
        |group by id, tumble(ts, interval '10' second)
      """.stripMargin)

    // 3.2 开窗函数
    // 3.2.1 Table API
    val overResultTable = sensorTable
      .window( Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow )
      .select( 'id, 'ts, 'id.count over 'ow, 'temp.avg over 'ow )

    // 3.2.2 SQL
    val overResultSqlTable = tableEnv.sqlQuery(
      """
        |select id, ts, count(id) over ow, avg(temp) over ow
        |from sensor
        |window ow as (
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
      """.stripMargin)



//    sensorTable.printSchema()
//    sensorTable.toAppendStream[Row].print()

    // 转成流打印输出
//    resultTable.toAppendStream[Row].print()
//    resultSqlTable.toAppendStream[Row].print("sql")

    overResultTable.toAppendStream[Row].print("over")
    overResultSqlTable.toAppendStream[Row].print("over-sql")

    env.execute("time and window test job")
  }
}
