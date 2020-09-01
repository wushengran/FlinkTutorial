package com.atguigu.api

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api
  * Version: 1.0
  *
  * Created by wushengran on 2020/9/1 16:58
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    new MyStateOperator
  }
}

class MyStateOperator extends RichMapFunction[SensorReading, String]{

  val myState = getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt", classOf[Int]))

  override def map(value: SensorReading): String = ""
}