package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * FlinkStudy
  * Create by Lianglei On 2021/06/14 09:54
  * Version 1.0
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val parameterTool = ParameterTool.fromArgs(args)
    val host = parameterTool.get("host")
    val port = parameterTool.getInt("port")
    val socketData:DataStream[String] = environment.socketTextStream(host,port)
    socketData.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print().setParallelism(1)

    environment.execute("stream word count")

  }

}
