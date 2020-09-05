package com.atguigu.api.tableapi.udftest

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.tableapi.udftest
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/5 9:43
  */
object AggregateFunctionTest {
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
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    // 1. Table API
    val avgTemp = new AvgTemp()
    val resultTable = sensorTable
      .groupBy('id)
      .aggregate( avgTemp('temp) as 'avgTemp )
      .select('id, 'avgTemp)

    resultTable.toRetractStream[Row].print("res")

    // 2. SQL
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select
        |id, avgTemp(temp)
        |from sensor
        |group by id
      """.stripMargin)

    resultSqlTable.toRetractStream[Row].print("sql")

    env.execute("agg function test job")
  }
}

// 专门定义一个聚合状态类型
class AvgTempAcc{
  var sum: Double = 0.0
  var count: Int = 0
}

// 自定义聚合函数
class AvgTemp extends AggregateFunction[Double, AvgTempAcc]{
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc()

  def accumulate( acc: AvgTempAcc, temp: Double ): Unit = {
    acc.sum += temp
    acc.count += 1
  }
}