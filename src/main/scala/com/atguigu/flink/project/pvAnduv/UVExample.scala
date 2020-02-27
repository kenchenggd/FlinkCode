package com.atguigu.flink.project.pvAnduv

import com.atguigu.flink.project.util.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object UVExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("F:\\flink-study\\src\\main\\resources\\UserBehavior.csv")
      .map(line =>{
        val arr = line.split(",")
        UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(r => ("pv", r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .process(new MyUvWindowFunction)
      .print()

    env.execute()


  }

  class MyUvWindowFunction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      //利用set集合进行去重：PV：每个userId可以重复，Uv：userID不能重复
      val userIdSet: mutable.Set[Long] = scala.collection.mutable.Set()
      for(ele <- elements){
          userIdSet.add(ele._2)
      }

      out.collect("窗口结束时间是：" + context.window.getEnd + "的窗口的uv是：" + userIdSet.size)

    }
  }

}
