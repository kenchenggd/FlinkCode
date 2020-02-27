package com.atguigu.flink.project.sourcefromdifferentapp

import java.util.{Calendar, UUID}

import com.atguigu.flink.project.util.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

//模拟数据源
class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true
  val channelSet = Seq("AppStore", "XiaomiStore")
  val behaviorTypes = Seq("BROWSE", "CLICK")
  val rand = new Random


  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    while (running) {

      //UUID 含义是通用唯一识别码 (Universally Unique Identifier)，这是一个软件建构的标准。
      //UUID保证对在同一时空中的所有机器都是唯一的。通常平台会提供生成的API。
      val userID = UUID.randomUUID().toString
      //Random.nextInt(int n):该方法的作用是生成一个随机的int值，该值介于[0,n)的区间，也就是0到n之间的随机int值，包含0而不包含n
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = Calendar.getInstance().getTimeInMillis
      sourceContext.collect(MarketingUserBehavior(userID, behaviorType, channel, ts))

      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}
