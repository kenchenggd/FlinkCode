package com.atguigu.flink.Window

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowFunctionExample {

  case class MinMaxTemp(id:String,min:Double,max:Double,endTs:Long)

  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      val sensorData = env.addSource(new SensorSource)

      val minMaxTemPerWindow = sensorData
          .keyBy(_.id)
          .timeWindow(Time.seconds(5))
          .process(new HighAndLowTempProcessFunction)

      minMaxTemPerWindow.print()
      env.execute()
    }



  class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading,MinMaxTemp,String,TimeWindow] {
    //窗口关闭的时候调用，Iterable包含了该时间窗口内的所有元素
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {
      val temaps = elements.map(_.temperature)
      //窗口关闭时间
      val windowEnd = context.window.getEnd

      out.collect(MinMaxTemp(key, temaps.max, temaps.min, windowEnd))
    }
  }

}
