package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/29 11:33
  */

// 批处理word count

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 从文件中读取数据
    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 3. 空格分词，然后map成计数的二元组进行聚合
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))    // 空格分割，打散成word
      .map( (_, 1) )    // map成(word, count)
      .groupBy(0)    // 以第一个元素，也就是word进行分组
      .sum(1)    // 对元组中第二个元素求和

    // 4. 打印输出
    resultDataSet.print()
  }
}
