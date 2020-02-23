package com.atguigu.flink.coprocessfunction

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object  CoprocessFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val reading = env.addSource(new SensorSource)

    val filterSwitches = env.fromCollection(Seq(
      ("sensor_2", 10 * 1000L)
    ))

    //Connect:DataStream,DataStream → ConnectedStreams：
    // 连接两个保持他们类型的数据流，两个数据流被Connect之后，
    // 只是被放在了一个同一个流中，内部依然保持各自的数据和形式不发生任何变化，
    // 两个流相互独立。
    reading.keyBy(_.id)
      .connect(filterSwitches.keyBy(_._1))
      .process(new ReadingProcess)
      .print()

    env.execute()

  }

  class ReadingProcess extends  CoProcessFunction[SensorReading,(String,Long),SensorReading]{

    lazy val forwardingEnable: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch",Types.of[Boolean])
    )

    lazy val disableTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",Types.of[Long])
    )

    override def processElement1(in1: SensorReading, context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {

      if(forwardingEnable.value()){
        collector.collect(in1)
      }

    }

    override def processElement2(in2: (String, Long), context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, collector: Collector[SensorReading]): Unit = {

      forwardingEnable.update(true)

      val timerTimerStamp = context.timerService().currentProcessingTime() + in2._2

      context.timerService().registerProcessingTimeTimer(timerTimerStamp)

      disableTimer.update(timerTimerStamp)

    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {

      forwardingEnable.clear()
      disableTimer.clear()

    }
  }

}
