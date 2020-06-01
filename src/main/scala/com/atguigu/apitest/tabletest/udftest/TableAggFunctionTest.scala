package com.atguigu.apitest.tabletest.udftest


import com.atguigu.apitest.SensorReading
import com.atguigu.apitest.tabletest.udftest.AggregateFunctionTest.AvgTemp
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.tabletest.udftest
  * Version: 1.0
  *
  * Created by wushengran on 2020/6/1 16:04
  */
object TableAggFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 先创建一个表聚合函数的实例
    val top2Temp = new Top2Temp()

    // Table API 调用
    val resultTable = sensorTable
      .groupBy('id)
      .flatAggregate( top2Temp('temperature) as ('temp, 'rank) )
      .select('id, 'temp, 'rank)

    resultTable.toRetractStream[Row].print("result")

    env.execute("agg udf test job")
  }

  // 自定义状态类
  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  // 自定义一个表聚合函数，实现Top2功能，输出（temp，rank）
  class Top2Temp() extends TableAggregateFunction[(Double, Int), Top2TempAcc]{
    // 初始化状态
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

    // 每来一个数据后，聚合计算的操作
    def accumulate( acc: Top2TempAcc, temp: Double ): Unit ={
      // 将当前温度值，跟状态中的最高温和第二高温比较，如果大的话就替换
      if( temp > acc.highestTemp ){
        // 如果比最高温还高，就排第一，其它温度依次后移
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if( temp > acc.secondHighestTemp ){
        acc.secondHighestTemp = temp
      }
    }

    // 实现一个输出数据的方法，写入结果表中
    def emitValue( acc: Top2TempAcc, out: Collector[(Double, Int)] ): Unit ={
      out.collect((acc.highestTemp, 1))
      out.collect((acc.secondHighestTemp, 2))
    }
  }
}
