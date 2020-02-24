package com.atguigu.flink.trigger

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/*一个触发器在窗口结束时间之前触发。当第一个事件被分配到窗口时，
这个触发器注册了一个定时器，定时时间为水位线之前一秒钟。
当定时事件执行，将会注册一个新的定时事件，这样，这个触发器每秒钟最多触发一次*/
object TriggerExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.addSource(new SensorSource)

    stream.assignAscendingTimestamps(r => r.timestamp)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .trigger(new OneSecondIntervalTrigger)
      .process(new TriggerWindowFunction)
      .print()

    env.execute()

  }

  class OneSecondIntervalTrigger extends Trigger[SensorReading,TimeWindow]{
    override def onElement(t: SensorReading, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      val firstSeen: ValueState[Boolean] = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",Types.of[Boolean])
      )
      if(!firstSeen.value()){
        val t = triggerContext.getCurrentWatermark + (1000 - (triggerContext.getCurrentWatermark % 1000))
        triggerContext.registerEventTimeTimer(t)
        triggerContext.registerEventTimeTimer(w.getEnd)
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, context: Trigger.TriggerContext): TriggerResult = {

      println(time.toString)
      if (time == window.getEnd){
        TriggerResult.FIRE_AND_PURGE
      }else{
        val t = context.getCurrentWatermark + (1000 - context.getCurrentWatermark % 1000)
        if(t < window.getEnd){
          context.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE
      }

    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {


      //当窗口被删除时，为了清空所有状态，触发器的clear()方法需要需要删掉所有的自定义per-window state，以及使用TriggerContext对象将处理时间和事件时间的定时器都删除
      val firstSeen: ValueState[Boolean] = triggerContext.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",Types.of[Boolean])
      )
      firstSeen.clear()

    }
  }

  class TriggerWindowFunction extends ProcessWindowFunction[SensorReading,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {

      out.collect("触发器触发，当前的key为：" + key + "  context.window.getEnd.toString： " + context.window.getEnd.toString + "元素个数为：" + elements.size.toString)

    }
  }

}
