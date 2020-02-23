package com.atguigu.flink.CoMapFunction

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ConnectStreamExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      (1,"a"),
      (1,"b")
    )

    val stream2 = env.fromElements(
      (1,1),
      (1,2)
    )

    //key相同的stream才能connect
    val conn = stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))

    val conn1 = stream1.connect(stream2)
      .keyBy(0,0)

    val outStream  = conn.map(new MyCoMapFunction)

    outStream.print()

    env.execute()

  }
  //connect合并两条不同类型的流到一条流
  //DataStream -> ConnectedStrem
  //但是两条流的类型是不一样的
  //故需要CoMapFunction合并成相同类型的数据

  class MyCoMapFunction extends CoMapFunction[(Int,String),(Int,Int),String]{
    override def map1(in1: (Int, String)): String = in1._2 + "：from map1"

    override def map2(in2: (Int, Int)): String = in2._2 + "：from map2"
  }

}
