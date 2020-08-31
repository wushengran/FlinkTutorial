package com.atguigu.api

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 9:16
  */

// 创建输入数据的样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest{
  def main(args: Array[String]): Unit = {
    // 创建流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 从集合读取数据
    val sensorList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1),
      SensorReading("sensor_1", 1547718199, 38.3),
      SensorReading("sensor_1", 1547718199, 35.1),
      SensorReading("sensor_1", 1547718199, 36.2)
    )
    val inputStream1: DataStream[SensorReading] = env.fromCollection(sensorList)

    env.fromElements(0, 0.67, "hello")

//    inputStream1.print("stream1").setParallelism(1)

    // 2. 从文件读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream2 = env.readTextFile(filePath)

//    inputStream2.print("stream2")

    // 3. 从kafka读取数据
//    env.socketTextStream("localhost", 7777)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")

    val inputStream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

//    inputStream3.print("stream3")

    // 4. 自定义source
    val inputStream4 = env.addSource( new MySensorSource() )
    inputStream4.print()

    // 执行
    env.execute("source test")
  }
}

// 自定义的SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标识位，用来指示是否正常生成数据
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()

    // 随机初始化10个传感器的温度值，之后在此基础上随机波动
    var curTempList = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    // 无限循环，生成随机的传感器数据
    while(running){
      // 在之前温度基础上随机波动一点，改变温度值
      curTempList = curTempList.map(
        curTempTuple => (curTempTuple._1, curTempTuple._2 + rand.nextGaussian())
      )
      // 获取当前的时间戳
       val curTs = System.currentTimeMillis()
      // 将数据包装成样例类，用ctx输出
      curTempList.foreach(
        curTempTuple => ctx.collect(SensorReading(curTempTuple._1, curTs, curTempTuple._2))
      )
      // 间隔1s
      Thread.sleep(1000)
    }
  }
}