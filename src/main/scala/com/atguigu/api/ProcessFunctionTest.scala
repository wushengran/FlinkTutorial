package com.atguigu.api

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/2 11:34
  */
object ProcessFunctionTest {
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

    // process function示例
//    val processedStream = dataStream
//      .keyBy("id")
//      .process(new MyKeyedProcess)
//
//    processedStream.getSideOutput(new OutputTag[Int]("side")).print("side")

    // 需求：检测10秒钟内温度是否连续上升，如果上升，那么报警
    val warningStream = dataStream
      .keyBy("id")
      .process( new TempIncreaseWarning(10000L) )

    warningStream.print()
    env.execute("process function test")
  }
}

// 自定义Keyed Process Function，实现10秒内温度连续上升报警检测
class TempIncreaseWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{
  // 首先定义状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer-ts", classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出状态
    val lastTemp = lastTempState.value()
    val curTimerTs = curTimerTsState.value()

    lastTempState.update(value.temperature)

    // 如果温度上升，并且没有定时器，那么注册一个10秒后的定时器
    if( value.temperature > lastTemp && curTimerTs == 0 ){
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      // 更新timerTs状态
      curTimerTsState.update(ts)
    } else if( value.temperature < lastTemp ){
      // 如果温度下降，那么删除定时器
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      // 清空状态
      curTimerTsState.clear()
    }
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发，说明10秒内没有温度下降，报警
    out.collect(s"传感器 ${ctx.getCurrentKey} 的温度值已经连续 ${interval/1000} 秒上升了")
    // 清空定时器时间戳状态
    curTimerTsState.clear()
  }
}

// 自定义Keyed ProcessFunction
class MyKeyedProcess extends KeyedProcessFunction[Tuple, SensorReading, Int]{

  var myState: ListState[Int] = _

  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getListState(new ListStateDescriptor[Int]("my-state", classOf[Int]))
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    myState.get()
    myState.add(10)
    myState.update(new util.ArrayList[Int]())

    // 侧输出流
    ctx.output(new OutputTag[Int]("side"), 10)
    // 获取当前键
    ctx.getCurrentKey
    // 获取时间相关
    ctx.timestamp()
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 10)
    ctx.timerService().deleteProcessingTimeTimer(1000L)
  }

  // 定时器触发时的操作定义
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, Int]#OnTimerContext, out: Collector[Int]): Unit = {
    println("timer occur")
    ctx.output(new OutputTag[Int]("side"), 15)
    out.collect(23)
  }

  override def close(): Unit = {}
}