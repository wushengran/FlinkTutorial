package com.atguigu.api.sinkTest

import com.atguigu.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.api.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2020/8/31 16:16
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)

    // 基本转换
    val dataStream: DataStream[SensorReading] = inputStream
      .map( line => {
        val arr = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    // 写入redis
    // 定义jedis连接
    val config = new FlinkJedisPoolConfig.Builder()
        .setHost("localhost")
        .setPort(6379)
        .build()

    dataStream.addSink( new RedisSink[SensorReading]( config, new MyRedisMapper() ) )

    env.execute("redis sink job")
  }
}

// 定义一个写入redis的Mapper类
class MyRedisMapper() extends RedisMapper[SensorReading]{
  // 写入redis的命令，保存成Hash表 hset 表名 field value
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "sensor")

  override def getValueFromData(data: SensorReading): String = data.temperature.toString

  override def getKeyFromData(data: SensorReading): String = data.id
}