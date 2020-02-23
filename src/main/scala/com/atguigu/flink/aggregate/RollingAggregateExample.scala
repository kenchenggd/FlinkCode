package com.atguigu.flink.aggregate

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._

object RollingAggregateExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStreamSink[(Int, Int)] = env.fromElements(
      (1,3),
     (2,2),
    (1,2)
    )
      .keyBy(0)
        .sum(1)
        .print()

    env.execute()
  }

}
