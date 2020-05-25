package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/25 11:22
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 1. 基本转换操作：map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
      } )

    // 2. 聚合操作，首先按照id做分组，然后取当前id的最小温度
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .minBy("temperature")

    // 3. 复杂聚合操作，reduce，得到当前id最小的温度值，以及最新的时间戳+1
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce( (curState, newData) =>
        SensorReading( curState.id, newData.timestamp + 1, curState.temperature.min(newData.temperature)) )

    // 4. 分流操作，split/select，以30度为界划分高低温流
    val splitStream: SplitStream[SensorReading] = dataStream
      .split( data => {
        if( data.temperature > 30 )
          Seq("high")
        else
          Seq("low")
      } )
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

    // 5. 合流操作，connect/comap
    val highWarningStream: DataStream[(String, Double)] = highTempStream
      .map( data => (data.id, data.temperature) )
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = highWarningStream
      .connect( lowTempStream )
    val coMapStream: DataStream[(String, Double, String)] = connectedStreams
      .map(
        highWarningData => (highWarningData._1, highWarningData._2, "warning"),
        lowTempData => (lowTempData.id, lowTempData.temperature, "normal")
      )

    // union操作：可以同时合并多条流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)

    // 6. 自定义函数类
    val resultStream: DataStream[SensorReading] = dataStream
      .filter( new MyFilter() )

    // 打印输出
//    reduceStream.print("reduce")
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")
//    coMapStream.print("coMap")

    resultStream.print("result")

    env.execute("transform test job")

  }
}

// 自定义一个filter函数类
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

// 自定义一个RichMapFunction函数类
class MyRichMapper() extends RichMapFunction[SensorReading, String]{

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def map(value: SensorReading): String = value.id

  override def close(): Unit = super.close()
}