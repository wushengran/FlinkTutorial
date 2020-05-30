package com.atguigu.apitest.tabletest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/30 15:36
  */
object FsOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 2. 读取数据转换成流，map成样例类
    val filePath: String = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)
    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 3. 把流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature as 'temp, 'timestamp as 'ts)

    // 4. 进行表的转换操作
    // 4.1 简单查询转换
    val resultTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")
    // 4.2 聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    // 5. 将结果表输出到文件中
    tableEnv.connect(new FileSystem().path("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\output.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
//          .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")
    resultTable.insertInto("outputTable")
//    aggResultTable.insertInto("outputTable")

    env.execute("fs output test job")
  }
}
