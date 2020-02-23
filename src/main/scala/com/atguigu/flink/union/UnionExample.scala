package com.atguigu.flink.union

import org.apache.flink.streaming.api.scala._


object UnionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //union合并流：DataStream -> DataStream,可以合并多个流，新的流包含所有流的数据
    //流的顺序随机
    val stream1 = env.fromElements(1,4,4)
    val stream2 = env.fromElements(2,5)
    val stream3 = env.fromElements(3,6)

    stream1.union(stream2,stream3).print()
    env.execute()
  }

}
