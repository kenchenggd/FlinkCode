package com.atguigu.flink.project.sourcefromdifferentapp

import java.util.{Calendar, UUID}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

//数据源分渠道统计
object SourceFromDiffAppExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior != "UNINSTAL")
      .map(r => {
        ((r.channel,r.behavior),1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(5),Time.seconds(1))
      .process(new MarketingCountByChannel)

    stream.print()
    env.execute()

  }

  class MarketingCountByChannel extends ProcessWindowFunction[((String,String),Long),(String,Long,Long),(String,String),TimeWindow]{
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[(String, Long, Long)]): Unit = {

      out.collect((key._1,elements.size,context.window.getEnd))
    }
  }

}
