package com.atguigu.apitest.tabletest.udftest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest.udftest
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/1 15:38
  */
object AggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 先创建一个聚合函数的实例
    val avgTemp = new AvgTemp()

    // Table API 调用
    val resultTable = sensorTable
      .groupBy('id)
      .aggregate( avgTemp('temperature) as 'avgTemp )
      .select('id, 'avgTemp)

    resultTable.toRetractStream[Row].print("result")

    env.execute("agg udf test job")
  }

  // 专门定义一个聚合函数的状态类，用于保存聚合状态（sum，count）
  class AvgTempAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }
  // 自定义一个聚合函数
  class AvgTemp extends AggregateFunction[Double, AvgTempAcc]{

    override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum/accumulator.count

    override def createAccumulator(): AvgTempAcc = new AvgTempAcc()

    def accumulate(acc: AvgTempAcc, temp: Double): Unit ={
      acc.sum += temp
      acc.count += 1
    }
  }
}
