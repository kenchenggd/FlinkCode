package com.atguigu.flink.project.pvAnduv


import com.atguigu.flink.project.util.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



object PvExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("F:\\flink-study\\src\\main\\resources\\UserBehavior.csv")
      .map(u =>{
        val uarr = u.split(",")
        UserBehavior(uarr(0).toLong,uarr(1).toLong,uarr(2).toInt,uarr(3),uarr(4).toLong)
      })
        .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv")
        .map(r => ("pv",1L))
        .keyBy(_._1)
        .timeWindow(Time.minutes(60),Time.minutes(5))
        .aggregate(new MyCountAggPV,new FullWindowAgg)

    stream.print()

    env.execute()

  }

  class FullWindowAgg extends ProcessWindowFunction[Long,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间是 " + context.window.getEnd.toString + " 毫秒的窗口中 PV 是： " + elements.iterator.next)
    }
  }

  class MyCountAggPV extends AggregateFunction[(String,Long),Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: (String, Long), acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

}
