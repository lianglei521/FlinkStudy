package com.atguigu.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDate = "E:\\idea_workspace\\src\\main\\resources\\hello.txt"
    val lines = env.readTextFile(inputDate)
    val wordCount = lines.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    wordCount.print()
  }
}
