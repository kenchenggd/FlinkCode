package com.atguigu.flink.sideoutput

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object LateEventToSideOutputExample2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102",9999,'\n')
    val  mainStream = stream
      .map(r =>{
        val arr = r.split(",")
        (arr(0),arr(1).toLong * 1000)
      }).assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new MyKeyedProcessFunction)

    mainStream.getSideOutput(new OutputTag[(String,Long)]("laterdata")).print()
    env.execute()

  }


  class MyKeyedProcessFunction extends KeyedProcessFunction[String,(String,Long),(String,Long)]{

    lazy val lateReadingOut = new OutputTag[(String,Long)]("laterdata")

    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, collector: Collector[(String, Long)]): Unit = {

      if(i._2 < context.timerService().currentWatermark()){
        context.output(lateReadingOut,i)
      }else{
        collector.collect(i)
      }

    }

  }

}
