package com.atguigu.flink.sideoutput

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
//侧输出流：process function的side outputs功能可以产生多条流
//并且这些流的数据类型可以不一样
//一个side output可以定义为OutputTag[X]对象，
//X是输出流的数据类型
//process function可以通过Context对象发射一个事件到一个或者多个side outputs
object SideOutputExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val reading = env.addSource(new SensorSource)
      .keyBy(_.id)
    val monitorStreaming = reading.process(new FreezingMonitor)

    monitorStreaming.getSideOutput(new OutputTag[String]("freezing-alarms")).print()



    env.execute()

  }

  class FreezingMonitor extends KeyedProcessFunction[String,SensorReading,SensorReading]{

    //定义一个侧输出标签，侧输出流的类型是String
    lazy val freezingAlarmOutput = new OutputTag[String]("freezing-alarms")

    override def processElement(i: SensorReading,
                                context: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                collector: Collector[SensorReading]): Unit = {

      if(i.temperature < 60){
        //把温度60小于的数据输出到侧输出流
        context.output(freezingAlarmOutput,s"templuter less than 40 alarm :${i.id}")
      }
 //把温度大于40的数据返回
      collector.collect(i)

    }
  }

}
