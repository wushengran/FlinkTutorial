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
  * Created by wushengran on 2020/5/23 11:47
  */

// 流处理 word count，DataStream API
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(8)

    // 从命令参数中读取hostname和port
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    // 从socket文本流读取数据
    val inputDataStream: DataStream[String] = env.socketTextStream(hostname, port)

    // 对DataStream进行转换操作，得到word count结果
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .map( (_, 1))
      .keyBy(_._1)
      .sum(1)

    // 打印输出
    resultDataStream.print().setParallelism(1)

    // 启动job
    env.execute("stream word count job")
  }
}
