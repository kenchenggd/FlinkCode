package com.atguigu.flink.keyedstate

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedStateExample {

  //键控状态：keyedState
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)//键控状态是针对不同key的
      .flatMap(new TemperatureAlertFunction(1.7))
      .print()

    env.execute()

  }

  class TemperatureAlertFunction(d: Double) extends  RichFlatMapFunction[SensorReading,(String,Double,Double)] {

    private var lastTempState:ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {

      //等价于
   /*   lazy val lastTempState =  getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTempState",Types.of[Double])*/
      lastTempState = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTempState",Types.of[Double])
      )

    }

    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {

      val lastTemp = lastTempState.value()
      val tempDiff = (in.temperature - lastTemp).abs
      if(tempDiff > d){
        collector.collect((in.id,in.temperature,lastTemp))
      }
      this.lastTempState.update(in.temperature)
    }
  }

}
