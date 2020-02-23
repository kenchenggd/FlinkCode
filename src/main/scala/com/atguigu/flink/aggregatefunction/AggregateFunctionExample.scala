package com.atguigu.flink.aggregatefunction

import com.atguigu.flink.source.SensorSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFunctionExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val dataStream = env.addSource(new SensorSource)

    dataStream.map(s =>(
      s.id,s.temperature
    )).keyBy(_._1)
      .timeWindow(Time.seconds(16))
      .aggregate(new MyAggregate)
      .print()

    env.execute()

  }

  class MyAggregate extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double,Int)] {
    override def createAccumulator(): (String, Double, Int) = (",",0.0,0)

    override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
      (in._1,in._2 + acc._2,acc._3 + 1)
    }

    override def getResult(acc: (String, Double, Int)): (String, Double, Int) = {
      (acc._1,acc._2/acc._3,acc._3)
    }

    override def merge(acc: (String, Double, Int), acc1: (String, Double, Int)): (String, Double, Int) = {
      (acc._1,acc1._2 + acc._2 ,acc._3 + acc1._3)
    }
  }

}
