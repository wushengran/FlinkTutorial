package com.atguigu.api

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/2 14:37
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

    // 用侧输出流实现分流操作，定义主流为高温流
    val highTempStream = dataStream
      .process( new SplitStreamOperation(30.0) )

    highTempStream.print("high")
    val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low"))
    lowTempStream.print("low")

    env.execute("side output test job")
  }
}

// 自定义ProcessFunction，实现高低温的分流操作
class SplitStreamOperation(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if( value.temperature > threshold ){
      // 如果大于阈值，直接输出到主流（高温流）
      out.collect(value)
    } else {
      // 如果小于等于，输出到侧输出流（低温流）
      ctx.output(new OutputTag[(String, Double, Long)]("low"), (value.id, value.temperature, value.timestamp))
    }
  }
}