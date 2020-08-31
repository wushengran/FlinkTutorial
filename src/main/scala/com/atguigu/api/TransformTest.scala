package com.atguigu.api

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 10:55
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)

    // 1. 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )
//      .filter( data => data.id.startsWith("sensor_1") )
      .filter( new MyFilter("sensor_1") )

    // 2. 分组聚合
    // 2.1 简单滚动聚合，求每一个传感器所有温度值的最小值
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .minBy("temperature")

    // 2.2 一般化聚合，输出(id, 最新时间戳 + 1，最小温度值)
    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      .reduce( (curState, newData) =>
        SensorReading(newData.id, newData.timestamp + 1, curState.temperature.min(newData.temperature))
      )

    // 3. 多流转换
    // 3.1 分流
    val splitStream = dataStream
      .split( data => {
        if( data.temperature > 30 ) List("high") else List("low")
      } )

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low")

    // 3.2 连接两条流
    val warningStream = highTempStream.map( data => (data.id, data.temperature) )
    val connectedStreams = warningStream.connect( lowTempStream )
    val resultStream: DataStream[Any] = connectedStreams
        .map(
          warningData => (warningData._1, warningData._2, "high temp warning"),
          lowTempData => (lowTempData.id, "low temp")
        )

    val unionStream = highTempStream.union(lowTempStream, allTempStream)

    dataStream.print("data")
//    reduceStream.print("reduce")
//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("all")

//    resultStream.print("result")

    env.execute("transform test")
  }
}

// 自定义函数类
class MyMapper extends MapFunction[SensorReading, (String, Double)]{
  override def map(value: SensorReading): (String, Double) = (value.id, value.temperature)
}

class MyFilter(keyword: String) extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = value.id.startsWith(keyword)
}

// 富函数
class MyRichFlatMapper extends RichFlatMapFunction[SensorReading, String]{
  override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
    out.collect(value.id)
    out.collect("value")
//    getRuntimeContext
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}