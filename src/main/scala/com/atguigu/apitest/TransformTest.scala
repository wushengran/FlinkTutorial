package com.atguigu.apitest

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

    reduceStream.print("reduce")

    env.execute("transform test job")

  }
}
