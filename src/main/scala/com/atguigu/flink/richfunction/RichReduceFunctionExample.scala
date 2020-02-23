package com.atguigu.flink.richfunction

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.functions.RichReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object  RichReduceFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sensorData = env.addSource(new SensorSource)

        sensorData
     .map(r => (r.id,r))
      .keyBy(_._1)
     // .timeWindow(Time.seconds(15))
      .reduce(new MyRichReduceFunction)
      .print()

    env.execute()
  }

  class MyRichReduceFunction extends RichReduceFunction[(String,SensorReading)]{

    override def open(parameters: Configuration): Unit = {
      println("enter reduce function")
    }

    override def reduce(t: (String, SensorReading), t1: (String, SensorReading)): (String, SensorReading) = {
      val min = t._2.temperature.min(t1._2.temperature)
      (t._1,SensorReading(t._1,t1._2.timestamp,min))
    }

    override def close(): Unit = {
      println("close reduce function")
    }
  }

}
