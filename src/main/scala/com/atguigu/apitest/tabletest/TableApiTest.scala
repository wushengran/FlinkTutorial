package com.atguigu.apitest.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/30 11:28
  */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    /*
    // 1.1 老版本planner的流式查询
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 用老版本
      .inStreamingMode() // 流处理模式
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)
    // 1.2 老版本批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
    // 1.3 blink版本的流式查询
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)
    // 1.4 blink版本的批式查询
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSettings)
    */

    // 2. 连接外部系统，读取数据
    // 2.1 读取文件数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) // 定义从外部文件读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
    ) // 定义表的结构
      .createTemporaryTable("inputTable") // 在表环境注册一张表

    // 2.2 消费kafka数据
    tableEnv.connect(new Kafka()
      .version("0.11") // 定义版本
      .topic("sensor") // 定义主题
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 3. 表的查询转换
    val sensorTable: Table = tableEnv.from("inputTable")
    // 3.1 简单查询转换
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter( 'id === "sensor_1" )
    // 3.2 聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'count)

    val aggResultSqlTable: Table = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")

    // 测试输出
    resultTable.toAppendStream[(String, Double)].print("result")
    aggResultTable.toRetractStream[(String, Long)].print("agg result")

    env.execute("table api test job")
  }
}
