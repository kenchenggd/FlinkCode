package com.atguigu.flink.richfunction

import com.atguigu.flink.source.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object ReduceFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceStream = env.addSource(new SensorSource)
    sourceStream
      .map(t => (t.id,t.temperature))
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      .reduce((r1,r2) => (r1._1,r1._2.min(r2._2)))
      .print()

    env.execute()
  }

}
