package com.atguigu.apitest

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichReduceFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/27 10:45
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend( new MemoryStateBackend() )
//    env.setStateBackend( new FsStateBackend("") )
//    env.setStateBackend( new RocksDBStateBackend("") )

    // 从文件读取数据
    //    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultStream = dataStream
      .keyBy( "id" )
      .reduce( new MyStateTestFunc() )

    dataStream.print()

    env.execute("state test job")
  }
}

// 自定义状态测试的ReduceFunction
class MyStateTestFunc() extends RichReduceFunction[SensorReading]{
  // state 定义
  lazy val myValueState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("myValue", classOf[Double]) )
  lazy val myListState: ListState[String] = getRuntimeContext.getListState( new ListStateDescriptor[String]("myList", classOf[String]) )
  lazy val myMapState: MapState[String, Int] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Int]("myMap", classOf[String], classOf[Int]) )
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState( new ReducingStateDescriptor[SensorReading]("myReduce", new MyReduceFunc, classOf[SensorReading]) )

  val myValue: Double = _

  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    // 获取状态
    val myValue: Double = myValueState.value()
    import scala.collection.JavaConversions._
    val myList: Iterable[String] = myListState.get()
    myMapState.get("sensor_1")
    myReducingState.get()

    // 写入状态
    myValueState.update( 0.0 )
    myListState.add("hello flink")
    myMapState.put("sensor_1", 1)
    myReducingState.add(value2)

    value2
  }
}