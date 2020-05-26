package com.atguigu.apitest

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/26 10:53
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    // 从文件读取数据
//    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
//        .assignAscendingTimestamps( _.timestamp * 1000L )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.milliseconds(1000) ) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 开窗统计，统计每个传感器每15秒中的温度最小值
    val resultStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      //     .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(10)) )
      //     .countWindow( 10, 2 )
      //     .window( EventTimeSessionWindows.withGap(Time.minutes(1)) )
      .timeWindow(Time.seconds(15))
        .allowedLateness(Time.minutes(1))
        .sideOutputLateData(new OutputTag[SensorReading]("late"))
      .reduce( new MyReduceFunc() )
//      .aggregate( new MyAggFunc() )

    dataStream.print("data")
    resultStream.print("result")
    resultStream.getSideOutput(new OutputTag[SensorReading]("late")).print("late")

    env.execute("window test job")
  }
}

// 自定义ReduceFunction
class MyReduceFunc() extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading( value1.id, value2.timestamp, value1.temperature.min(value2.temperature) )
  }
}

// 自定义一个求温度平均值的AggregateFunction
class MyAggFunc extends AggregateFunction[SensorReading, (Double, Int), Double]{
  override def add(value: SensorReading, accumulator: (Double, Int)): (Double, Int) =
    ( accumulator._1 + value.temperature, accumulator._2 + 1 )

  override def createAccumulator(): (Double, Int) = (0.0, 0)

  override def getResult(accumulator: (Double, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Double, Int), b: (Double, Int)): (Double, Int) =
    ( a._1 + b._1, a._2 + b._2 )
}