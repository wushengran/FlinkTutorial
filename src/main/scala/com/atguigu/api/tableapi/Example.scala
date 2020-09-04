package com.atguigu.api.tableapi

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.tableapi
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/4 10:09
  */
object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })

    // 基于env创建一个表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于一条流创建一张表，流中的样例类的字段就对应着表的列
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 1. 调用table API，进行表的转换操作
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 2. 直接写sql，得到转换结果
    tableEnv.createTemporaryView("datatable", dataTable)
    val sql = "select id, temperature from datatable where id = 'sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sql)

//    resultTable.printSchema()
//    val resultStream = resultTable.toAppendStream[(String, Double)]
//    resultStream.print("res")
    val resultSqlStream = resultSqlTable.toAppendStream[(String, Double)]
    resultSqlStream.print("sql")

    env.execute("table api example")
  }
}
