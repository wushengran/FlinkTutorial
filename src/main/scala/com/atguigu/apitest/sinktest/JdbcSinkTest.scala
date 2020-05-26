package com.atguigu.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinktest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/26 9:14
  */
object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 将数据写入自定义sink
    dataStream.addSink( new MyJdbcSink() ).setParallelism(1)

    env.execute("jdbc sink test job")
  }
}

// 实现自定义的sink function
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 声明连接变量和预编译语句
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 创建连接和预编译语句
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
  }

  // 每来一条数据，就调用连接，执行一次sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 直接执行update语句，如果没有更新数据，那么执行insert
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