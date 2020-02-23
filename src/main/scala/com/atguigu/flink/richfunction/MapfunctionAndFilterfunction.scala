package com.atguigu.flink.richfunction

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

object MapfunctionAndFilterfunction {

  def main(args: Array[String]): Unit = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.addSource(new SensorSource)

    inputStream.map(new myMapFunction).print()
    //inputStream.map(s => s.id).print()
   // inputStream.filter(new MyFilterFunction).print()
    inputStream.filter(_.temperature > 60).print()
    env.execute()
  }

  class myMapFunction extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = t.id
  }

  class MyFilterFunction extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = t.temperature > 60
  }

}
