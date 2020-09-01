package com.atguigu.api.sinkTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.api.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 16:48
  */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
//    val inputStream: DataStream[String] = env.readTextFile(filePath)

    val inputStream4 = env.addSource( new MySensorSource() )

    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream4
//      .map( line => {
//        val arr = line.split(",")
//        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
//      } )

    // 写入mySQL
    dataStream.addSink( new MyJdbcSink() )

    env.execute("jdbc sink job")
  }
}

// 自定义实现SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 创建连接，并实现预编译语句
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
  }

  override def invoke(value: SensorReading, context: _root_.org.apache.flink.streaming.api.functions.sink.SinkFunction.Context[_]): Unit = {
    // 直接执行更新语句，如果没有更新就插入
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if( updateStmt.getUpdateCount == 0 ){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}