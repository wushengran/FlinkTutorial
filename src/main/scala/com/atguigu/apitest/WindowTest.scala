package com.atguigu.apitest

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

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 开窗统计
    val resultStream = dataStream
      .keyBy("id")
      //              .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(10)) )
      .timeWindow(Time.seconds(15), Time.seconds(5))
    //     .countWindow( 10, 2 )
    //      .window( EventTimeSessionWindows.withGap(Time.minutes(1)) )

    env.execute("window test job")
  }
}
