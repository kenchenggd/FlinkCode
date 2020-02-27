package com.atguigu.flink.project.sourcefromdifferentapp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//APP不分渠道统计
object AppMarketingStatics {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTALL")
      .map(r =>{
        ("dummyKey",1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5),Time.seconds(1))
      .process(new MarkCountTalFunction)

    stream.print()
    env.execute()

  }

  class MarkCountTalFunction extends  ProcessWindowFunction[(String,Long),(String,Long,Long),String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long, Long)]): Unit = {

      out.collect((key,elements.size.toLong,context.window.getEnd))

    }
  }

}
