package com.atguigu.flink.source

import java.util.Calendar

import com.atguigu.flink.Util.SensorReading
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random
//RichParallelSourceFunction:ParallelSourceFunction接口，同时继承了AbstractRichFunction
//AbstractRichFunction主要实现了RichFunction接口的setRuntimeContext、getRuntimeContext、getIterationRuntimeContext方法；open及close方法都是空操作
class SensorSource extends RichParallelSourceFunction[SensorReading]{

var running = true

  //生成上下文之后，接下来就是把上下文交给 SourceFunction 去执行,调用用户重写的run方法开始正式运行
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    //产生相应id（1-10）的温度值
    var curFTemp = (1 to 10)
      .map(
        i =>("sensor_" + i,65 + (rand.nextGaussian()*0.5))
      )
    while (running){
      curFTemp = curFTemp.map(
        t => (t._1,t._2 + (rand.nextGaussian()*0.5))
      )

      //时间,当前时间
      val curTime = Calendar.getInstance.getTimeInMillis

      //生产sensorReading
      curFTemp.foreach(t => sourceContext.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false

}
