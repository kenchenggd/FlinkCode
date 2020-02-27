package com.atguigu.flink.keyedstate

import com.atguigu.flink.Util.SensorReading
import com.atguigu.flink.keyedstate.KeyedStateExample.TemperatureAlertFunction
import com.atguigu.flink.source.SensorSource
import org.apache.flink.streaming.api.scala._

object KeyedStateExample2 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)//键控状态是针对不同key的
      .flatMapWithState[(String,Double,Double),Double]{
      case(in:SensorReading,None) =>
        (List.empty,Some(in.temperature))
      case (r:SensorReading,lastTemp: Some[Double]) =>
        val tempDiff = (r.temperature - lastTemp.get).abs
        if(tempDiff > 1.7){
          (List((r.id,r.temperature,lastTemp.get)),Some(r.temperature))
        }else
        //第一次或者或者状态变量lastTemp为空，定义赋值一个lastTemp
          (List.empty,Some(r.temperature))
        }
      .print()

    env.execute()


  }

}
