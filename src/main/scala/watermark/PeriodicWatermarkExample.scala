package watermark

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.source.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//周期产生watermark
object PeriodicWatermarkExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData = env.addSource(new SensorSource)
      //如果事件时间是单调递增而且有序，则设置watermark的延时为0
      //sensorData.assignAscendingTimestamps(t => t.timestamp)

    //水位线的添加在source之后，keyby之前
      sensorData.assignTimestampsAndWatermarks(
        new PeriodicAssigner //自定义类产生水位线，继承AssignerWithPeriodicWatermarks，里面产生watermark
      )
      .keyBy(_.id)
      .timeWindow(Time.seconds(6))
      .process(new MyWindow)
      .print()

    env.execute()




  }

  class PeriodicAssigner extends  AssignerWithPeriodicWatermarks[SensorReading]{
    //定义延迟时间
    val bound = 1000
    //保存观察到的最大时间时间戳
    var maxTs = Long.MinValue

    //系统默认200s插入一次水位线
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs - bound)
    }

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
      maxTs = maxTs.max(t.timestamp)
      t.timestamp
    }
  }

  class MyWindow extends  ProcessWindowFunction[SensorReading,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {

      out.collect("在该窗口，" + key + "的数据有"  + elements.size.toString + "个")

    }
  }

}
