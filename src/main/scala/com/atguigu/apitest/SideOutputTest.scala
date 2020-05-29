package com.atguigu.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/29 9:30
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 用侧输出流实现一个分流操作
    val highTempStream: DataStream[SensorReading] = dataStream
      .process( new SplitTempMonitor() )

    // 获取两条流输出
    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("lowtemp")).print("low")

    env.execute("side output test job")

  }
}

// 实现自定义ProcessFunction做分流
class SplitTempMonitor() extends ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 判断当前数据的温度值，如果大于等于30度就输出到主流，如果小于30度输出到侧输出流
    if( value.temperature >= 30 )
      out.collect( value )
    else
      ctx.output( new OutputTag[(String, Double, Long)]("lowtemp"), (value.id, value.temperature, value.timestamp) )
  }
}