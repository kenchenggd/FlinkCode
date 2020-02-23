package com.atguigu.flink.keyedprocessfunction

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedProceeFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val warning = env.addSource(new SensorSource)
      .keyBy(_.id)
      .process(new MyKeyedProcess)

    warning.print()
    env.execute()
  }

  class MyKeyedProcess extends KeyedProcessFunction[String,SensorReading,String] {

    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemper", Types.of[Double])
    )

    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

      val prevTemp = lastTemp.value()

      lastTemp.update(value.temperature)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0D || value.temperature < prevTemp) {
        context.timerService().deleteEventTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      } else if (value.temperature > prevTemp && curTimerTimestamp == 0) {
        val timerTs = context.timerService().currentProcessingTime() + 1000
        context.timerService().registerProcessingTimeTimer(timerTs)

        currentTimer.update(timerTs)
      }

    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

      out.collect(ctx.getCurrentKey + "温度已经连续1s上升")
      currentTimer.clear()

    }

  }

}
