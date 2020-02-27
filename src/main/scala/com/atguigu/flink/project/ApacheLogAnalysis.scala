package com.atguigu.flink.project



import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.atguigu.flink.project.util.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ApacheLogAnalysis {

  def main(args: Array[String]): Unit = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("F:\\flink-study\\src\\main\\resources\\apachetest.log")
      .map(line =>{
        val arr =  line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestam = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0),arr(2),timestam,arr(5),arr(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
          override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .aggregate(new CountAgg,new WindowResultFunction)
      .keyBy(_.windowEnd)
      .process(new MyKeyedProcessTop(5))

    stream.print()
    env.execute()


  }

  class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key,context.window.getEnd,elements.iterator.next()))
    }
  }

  class MyKeyedProcessTop(i: Int) extends  KeyedProcessFunction[Long,UrlViewCount,String]{

    lazy val urlList: ListState[UrlViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("urlListState",Types.of[UrlViewCount])
    )

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

      urlList.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {


      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()


      import  scala.collection.JavaConversions._
      for(urlView <- urlList.get){
        allUrlViews += urlView
      }

      urlList.clear()

      val sortedUrlViews = allUrlViews.sortBy(-_.count).take(i)

      var result = new StringBuilder

      result.append("================================")
        .append("时间：")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for(i <- sortedUrlViews.indices){
        val currentUrlView = sortedUrlViews(i)
        result.append("No")
          .append(i + 1)
          .append(":")
          .append("URL = ")
          .append(currentUrlView.url)
          .append("\n")
          .append("流量 = ")
          .append(currentUrlView.count)
          .append("\n")
      }
      result.append("================================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())

    }
  }

}
