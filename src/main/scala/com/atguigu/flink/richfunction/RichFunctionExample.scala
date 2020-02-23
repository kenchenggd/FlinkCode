package com.atguigu.flink.richfunction

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //所有的Flink函数类都有其Rich版本，可以获取运行环境的上下文（getRuntimeContext()
    // 提供了函数的RuntimeContext 的一些信息，例如函数执行的并行度，任务的名字，以及state状态）
    env.fromElements(1,2,3)
      .map(new MyRichMapFunction)
      .print()

    env.execute()



  }

  class MyRichMapFunction extends  RichMapFunction[Int,Int]{
    override def open(parameters: Configuration): Unit = {
      println("enter map function!")
    }

    override def map(value: Int): Int = value + 1

    override def close(): Unit = {

      println("close map function")

    }
  }

}
