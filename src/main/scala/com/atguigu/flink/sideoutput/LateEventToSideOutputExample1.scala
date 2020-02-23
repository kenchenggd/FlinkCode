package com.atguigu.flink.sideoutput


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


//窗口迟到的数据保存在侧输出流中
object LateEventToSideOutputExample1 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("hadoop102", 9999, '\n')
   // stream.print()
    val mainStream = stream.map(r => {
      val arr = r.split(",")
      (arr(0), arr(1).toLong * 1000)
    })
      .assignAscendingTimestamps(_._2)//默认的毫秒，故arr(1).toLong * 1000必须*1000
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .sideOutputLateData(new OutputTag[(String,Long)]("delay data"))
      .process(new MyWindowProcess)
    mainStream.print()
   //OutputTag[X]对象，X是输出流的数据类型
   mainStream.getSideOutput(new OutputTag[(String,Long)]("delay data")).print()

    env.execute()


  }

  class MyWindowProcess extends  ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      out.collect("正常输出的" + key + "有：" + elements.size.toString)
    }

  }

}
