package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/29 11:50
  */

// 流处理word count

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(10)
//    env.disableOperatorChaining()

    // 2. 接收socket文本流
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)

    // 3. 对DataStream进行转换处理
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty).slotSharingGroup("1")
      .map( (_, 1) ).disableChaining()
      .keyBy(0)
      .sum(1).startNewChain()

    // 4. 打印输出
    resultDataStream.print().setParallelism(1)

    // 5. 启动执行任务
    env.execute("stream word count")
  }
}
