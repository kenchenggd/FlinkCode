package com.atguigu.flink.eventtime


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102",9999,'\n')

    stream.map(r => {
      val arr = r.split(",")
      (arr(0),arr(1).toLong*1000)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        override def extractTimestamp(t: (String, Long)): Long = t._2
      }).keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new WaterMarkProcessFunction)
      .print()

    env.execute()

  }

  class WaterMarkProcessFunction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit ={
      out.collect("窗口中共有" + elements.size.toString + "条数据")
    }

  }

}
