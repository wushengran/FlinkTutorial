package com.atguigu.apitest.sinktest

import java.util

import com.atguigu.apitest.SensorReading
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
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/25 16:47
  */
object esSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 定义httphosts
    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add( new HttpHost("localhost", 9200) )

    // 定义es sink function
    val esSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 首先定义写入es的source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", element.id)
        dataSource.put("temp", element.temperature.toString)
        dataSource.put("ts", element.timestamp.toString)

        // 创建index request
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("data")
          .source(dataSource)

        // 使用RequestIndexer发送http请求
        indexer.add(indexRequest)

        println("data " + element + " saved successfully")
      }
    }

    dataStream.addSink( new ElasticsearchSink
      .Builder[SensorReading](httpHosts, esSinkFunc)
      .build())

    env.execute("es sink test job")
  }
}
