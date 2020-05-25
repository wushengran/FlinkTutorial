package com.atguigu.apitest

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
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/25 10:08
  */

// 定义温度传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection( List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
//    val stream = env.fromElements(1, 0.435, "hello", ("word", 1))

    // 2. 从文件读取数据
    val stream2: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3. 从kafka里读取数据
    // 先创建kafka的相关配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3: DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    // 4. 自定义source
    val stream4: DataStream[SensorReading] = env.addSource( new MySensorSource() )

    // 打印输出
//    stream1.print("stream1")
//    stream2.print("stream2")
    stream4.print("stream4")
    // 执行job
    env.execute("source test job")
  }
}

// 自定义生成测试数据源的SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标识位，用来表示数据源是否正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }
  // 随机生成10个传感器的温度数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 创建一个随机数生成器
    val rand = new Random()

    // 初始化10个传感器的温度值，随机生成，包装成二元组（id, temperature）
    var curtemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    // 无限循环生成数据，如果cancel的话就停止
    while(running){
      // 更新当前温度值，再之前温度上增加微小扰动
      curtemp = curtemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳，包装样例类
      val curTs = System.currentTimeMillis()
      curtemp.foreach(
        data => ctx.collect( SensorReading( data._1, curTs, data._2 ) )
      )
      // 间隔200ms
      Thread.sleep(200)
    }
  }
}