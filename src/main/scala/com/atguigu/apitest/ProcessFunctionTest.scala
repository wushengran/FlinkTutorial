package com.atguigu.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/5/27 15:38
  */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 自定义ProcessFunction，进行有状态的复杂处理
    val resultStream = dataStream
      .keyBy("id")
//      .process( new MyKeyedProcessFunction() )
      .process( new TempIncreWarning(10000L) )

    resultStream.print()

    env.execute("process function test job")
  }
}

// 自定义Process Function，检测10秒之内温度连续上升
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String]{
  // 定义一个ValueState，用来保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  // 定义一个状态，用来保存设置的定时器时间戳
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("cur-timer", classOf[Long]) )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一次温度值
    val lastTemp: Double = lastTempState.value()
    val curTimer: Long = curTimerState.value()

    // 更新温度值状态
    lastTempState.update(value.temperature)

    // 将当前的温度值，跟上次的比较
    if( value.temperature > lastTemp && curTimer == 0){
      // 如果温度上升，且没有注册过定时器，那么按当前时间加10s注册定时器
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerState.update(ts)
    } else if( value.temperature < lastTemp ){
      // 如果温度下降，那么直接删除定时器，重新开始
      ctx.timerService().deleteProcessingTimeTimer(curTimer)
      curTimerState.clear()
    }
  }
  // 定时器触发，说明连续10秒内没有温度下降，直接报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect( "传感器 " + ctx.getCurrentKey + " 的温度值连续" + interval / 1000 + "秒上升" )
    // 清空timer状态
    curTimerState.clear()
  }
}

// Process Function示例
class MyKeyedProcessFunction() extends KeyedProcessFunction[Tuple, SensorReading, String]{
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.output(new OutputTag[String]("side"), value.id)
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}