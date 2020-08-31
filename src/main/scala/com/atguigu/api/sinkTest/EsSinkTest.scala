package com.atguigu.api.sinkTest

import java.util

import com.atguigu.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 16:30
  */
object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)

    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    // 写入es
    // 定义HttpHost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    dataStream.addSink( new ElasticsearchSink
        .Builder[SensorReading](httpHosts, new MyEsSinkFunction)
        .build()
    )

    env.execute("es sink job")
  }
}

// 定义一个EsSinkFunction
class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading]{
  override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
    // 提取数据包装source
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", element.id)
    dataSource.put("temp", element.temperature.toString)
    dataSource.put("ts", element.timestamp.toString)

    // 创建index request
    val indexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("temperature")
      .source(dataSource)

    // 利用RequestIndexer发送http请求
    indexer.add(indexRequest)

    println(element + " saved")
  }
}