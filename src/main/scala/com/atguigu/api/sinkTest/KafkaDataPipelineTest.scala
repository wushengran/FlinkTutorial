package com.atguigu.api.sinkTest

import java.util.Properties

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 15:47
  */
object KafkaDataPipelineTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")

    val inputStream = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    // 写入kafka
    dataStream.map( data => data.toString )
      .addSink( new FlinkKafkaProducer011[String]("localhost:9092", "sinktest", new SimpleStringSchema()) )

    env.execute("kafka pipeline job")
  }
}
