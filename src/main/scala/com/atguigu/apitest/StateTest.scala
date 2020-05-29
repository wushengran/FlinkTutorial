package com.atguigu.apitest

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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
//    env.enableCheckpointing(1000L)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
//    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
//
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
//    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // 从文件读取数据
    //    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 用自定义RichFunction实现状态编程
    val resultStream: DataStream[(String, Double, Double)] = dataStream
      .keyBy( "id" )
//      .reduce( new MyStateTestFunc() )
      .flatMap( new TempChangeAlert(10.0) )

    // 用已有的api实现状态编程
    val resultStream2: DataStream[(String, Double, Double)] = dataStream
      .keyBy("id")
      .flatMapWithState[(String, Double, Double), Double]( {
      case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
      case (inputData: SensorReading, lastTemp: Some[Double]) => {
        val diff = ( inputData.temperature - lastTemp.get ).abs
        if( diff > 10.0 ){
          ( List( (inputData.id, lastTemp.get, inputData.temperature) ), Some(inputData.temperature) )
        } else {
          ( List.empty, Some(inputData.temperature) )
        }
      }
    } )

    dataStream.print("data")
    resultStream2.print("alert")

    env.execute("state test job")
  }
}

// 自定义RichFlatMapFunction
class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义一个状态，用来保存上一次的温度值
  //  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  private var lastTempState: ValueState[Double] = _
  private var isFirstTempState: ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
    isFirstTempState = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("is-firsttemp", classOf[Boolean], true) )
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取状态，拿到上次的温度值
    val lastTemp = lastTempState.value()

    // 更新状态，保存当前的温度值
    lastTempState.update(value.temperature)

    // 对比两次温度值，如果大于阈值，报警
    val diff = (value.temperature - lastTemp).abs
    if( diff > threshold && !isFirstTempState.value() ){
      out.collect( (value.id, lastTemp, value.temperature) )
    }
    isFirstTempState.update(false)
  }
}


// 自定义状态测试的ReduceFunction
class MyStateTestFunc() extends RichReduceFunction[SensorReading]{
  // state 定义
  lazy val myValueState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("myValue", classOf[Double]) )
  lazy val myListState: ListState[String] = getRuntimeContext.getListState( new ListStateDescriptor[String]("myList", classOf[String]) )
  lazy val myMapState: MapState[String, Int] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Int]("myMap", classOf[String], classOf[Int]) )
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState( new ReducingStateDescriptor[SensorReading]("myReduce", new MyReduceFunc, classOf[SensorReading]) )

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