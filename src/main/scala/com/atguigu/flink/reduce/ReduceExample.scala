package com.atguigu.flink.reduce

import org.apache.flink.streaming.api.scala._

object ReduceExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
  /*
    val l1 = List("a")
    val l2 = List("b")
    println(l1 ::: l2)//':::'合并两个List为一个List*/

    val inputStream = env.fromElements(
      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")), ("fr", List("je"))
    )
    inputStream.keyBy(0)
      .reduce((x,y) => (x._1,x._2 ::: y._2))
      .print()

    env.execute()



  }

}
