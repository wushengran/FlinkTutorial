package com.atguigu.api.tableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.tableapi
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/4 10:41
  */
object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1. 表执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    /*
    // 1.1 老版本的流处理环境
    val oldStreamSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings)

    // 1.2 老版本批处理环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 blink流处理环境
    val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 blink批处理环境
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
    */

    // 2. 创建表
    // 从文件中读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"

    tableEnv.connect( new FileSystem().path(filePath) )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val sensorTable = tableEnv.from("inputTable")
//    sensorTable.toAppendStream[(String, Long, Double)].print()

    // 连接到Kafka
    tableEnv.connect( new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE()) )
      .createTemporaryTable("kafkaInputTable")

    // 3. 表的查询
    // 3.1 Table API
    // 3.1.1 简单查询
    val resultTable = sensorTable
      .select('id, 'temperature)
      .where( 'id === "sensor_1" )

    // 3.1.2 聚合查询
    val aggResultTable = sensorTable
      .groupBy('id)    // 按照id分组
      .select('id, 'id.count as 'count)

    // 3.2 SQL
    // 3.2.1 简单查询
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_1'
      """.stripMargin)

    // 3.2.2 聚合查询
    val aggResultSqlTable = tableEnv.sqlQuery(
      """
        |select id, count(id) as cnt
        |from inputTable
        |group by id
      """.stripMargin)

//    resultTable.toAppendStream[(String, Double)].print("res")
//    resultSqlTable.toAppendStream[(String, Double)].print("sql")
//    aggResultTable.printSchema()
//    aggResultTable.toRetractStream[(String, Long)].print("res")
//    aggResultSqlTable.toRetractStream[(String, Long)].print("sql")

    // 4. 输出
    // 输出到文件
    val outputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\output.txt"

    tableEnv.connect( new FileSystem().path(outputPath) )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
//        .field("temperature", DataTypes.DOUBLE())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")
    resultTable.insertInto("outputTable")
//    aggResultTable.insertInto("outputTable")

    env.execute("table api test")
  }
}
