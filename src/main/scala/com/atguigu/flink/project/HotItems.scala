package com.atguigu.flink.project



import java.sql.Timestamp

import com.atguigu.flink.project.util.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("F:\\flink-study\\src\\main\\resources\\UserBehavior.csv")
      .map(u =>{
        val uarr = u.split(",")
        UserBehavior(uarr(0).toLong,uarr(1).toLong,uarr(2).toInt,uarr(3),uarr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new MyAggFunction,new MyProcessWindowFunction)
      .keyBy(_.windowEnd)
        .process(new TopHotItems(3))

    stream.print()

    env.execute()

  }

  class MyAggFunction extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class MyProcessWindowFunction extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.iterator.next()))
    }
  }

  class MyItemWindowFunction

  class TopHotItems(i: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {

    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items",Types.of[ItemViewCount])
    )

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val allItems: ListBuffer[ItemViewCount] = ListBuffer()


      import  scala.collection.JavaConversions._
      for(item <- itemState.get){
        allItems += item
      }
      itemState.clear()

      val sortItems = allItems.sortBy(-_.count).take(3)
      val result = new StringBuilder
      result
        .append("=============================")
        .append("时间： ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")
        for( i <- sortItems.indices){
          val currentItem = sortItems(i)
          result
            .append("No")
            .append(i + 1)
            .append(":")
            .append("商品ID = ")
            .append(currentItem.itemId)
            .append("浏览量=")
            .append(currentItem.count)
            .append("\n")
        }
      result.append("==============================")
      out.collect(result.toString())
    }
  }

}
