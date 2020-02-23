package com.atguigu.flink.allowedlateness

import com.atguigu.flink.sideoutput.LateEventToSideOutputExample1.MyWindowProcess
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//把窗口的关闭时间延迟一定时间，防止窗口关闭时间到了数据丢失
object AllowedLatenessExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102",9999,'\n')

    stream.map(r =>{
      val arr = r.split(",")
      (arr(0),arr(1).toLong * 1000)
    }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        override def extractTimestamp(t: (String, Long)): Long = t._2
      }
    ).keyBy(_._1)
      .timeWindow(Time.seconds(5))
      //表示窗口延迟5s关闭，但触发计算是在水位线到了原来窗口定义的关闭时间的时候
      .allowedLateness(Time.seconds(5))//水位线到了窗口触发时间会触发相应操作，但窗口要延迟一段时间才关闭，即并不是水位线 = 窗口时间 + 延迟时间才触发计算
      .process(new MyWindowProcess1)
      .print()

    env.execute()
  }

  class MyWindowProcess1 extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      val cnt = elements.size

      lazy  val isUpadate = context.windowState
        .getState(
          new ValueStateDescriptor[Boolean]("isUpdate",Types.of[Boolean])
        )

      if(!isUpadate.value()){
        out.collect("到了窗口原来关闭时间，数据总数为：" + cnt.toString)
        isUpadate.update(true)
      }else{
        out.collect("窗口延迟以后的时间内到达的数据个数为：" + cnt.toString)
      }

    }
  }

}
