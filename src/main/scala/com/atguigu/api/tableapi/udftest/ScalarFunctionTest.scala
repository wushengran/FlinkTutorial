package com.atguigu.api.tableapi.udftest

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.tableapi.udftest
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/5 9:17
  */
object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv = StreamTableEnvironment.create(env)

    // 读取数据
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = inputStream
      .map(line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 将流转换成Table，同时设定时间字段
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime as 'ts, 'temperature as 'temp)

    // 1. Table API中使用
    // 创建UDF实例，然后直接使用
    val hashCode = new HashCode(5)
    val resultTable = sensorTable
      .select( 'id, 'ts, 'temp, hashCode('id) )

    resultTable.toAppendStream[Row].print("res")

    // 2. SQL中使用
    // 在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id, ts, temp, hashCode(id) from sensor")

    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar function test job")
  }
}

// 自定义一个标量函数
class HashCode( factor: Int ) extends ScalarFunction {
  def eval( value: String ): Int = {
    value.hashCode * factor
  }
}