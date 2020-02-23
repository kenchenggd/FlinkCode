package com.atguigu.flink.Window


import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//增量聚合函数和全窗口聚合函数的结合使用:
//增量聚合函数没有无法获取processtime，而把所有数据拉到ProcessWindowFunction里面处理，大数据量时可能导致OOM
//解决办法：先用增量聚合函数聚合出来最大值，最小值，再把聚合之后的值传入ProcessWindowFunction，ProcessWindow只是添加当前的processtime
object AggregateWithProcessWindowExample {

  case class MinMaxTemp(id:String,min:Double,max:Double,time:Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sensorData = env.addSource(new SensorSource)

    val minMaxTempPerWindow = sensorData
      .map(r =>(r.id,r.temperature))
      .keyBy(_._1)
        .timeWindow(Time.seconds(5))
        .aggregate(new Myagg,new AssigWindowEndProcessFunction)

    minMaxTempPerWindow.print()

    env.execute()
  }

  class Myagg extends  AggregateFunction[(String,Double),(String,Double,Double),(String,Double,Double)]{

    override def createAccumulator(): (String, Double, Double) = ("",Double.MaxValue,Double.MinValue)

    override def add(in: (String, Double), acc: (String, Double, Double)): (String, Double, Double) = {
      (in._1,acc._2.min(in._2),acc._2.max(in._2))
    }

    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc

    override def merge(acc: (String, Double, Double), acc1: (String, Double, Double)): (String, Double, Double) = {
      (acc._1,acc1._2.min(acc._2),acc1._2.max(acc._2))
    }
  }

  class AssigWindowEndProcessFunction extends  ProcessWindowFunction[(String,Double,Double),MinMaxTemp,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val e = elements.iterator.next()
      out.collect(MinMaxTemp(key,e._2,e._3,context.window.getEnd))
    }
  }

}
