package com.atguigu.api.sinkTest

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 15:30
  */
object FileSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setParallelism(1)

    // 读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)

    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    // 写入文件
//    dataStream.writeAsCsv("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\out.txt"),
        new SimpleStringEncoder[SensorReading]("UTF-8")
      ).build()
    )

    env.execute("file sink job")
  }
}
