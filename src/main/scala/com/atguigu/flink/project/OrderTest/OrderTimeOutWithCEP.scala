package com.atguigu.flink.project.OrderTest

import com.atguigu.flink.project.util.OrderEvent
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import  scala.collection.Map
//使用CEP检测支付超时
object OrderTimeOutWithCEP {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.fromCollection(List(
        OrderEvent("1", "create", "1558430842"),
        OrderEvent("2", "create", "1558430843"),
        OrderEvent("2", "pay", "1558430844"),
        OrderEvent("3", "pay", "1558430942"),
        OrderEvent("4", "pay", "1558430943")
    ))
      .assignAscendingTimestamps(_.ts.toLong * 1000)

    //5s内支付成功
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.op.equals("create"))
      .next("next")
      .where(_.op.equals("pay"))
      .within(Time.seconds(5))

    //5s内支付超时放到侧输出流
    val orderTimeOutPut = OutputTag[OrderEvent]("orderTimeOut")

    //关联
    val patternStream = CEP.pattern(stream.keyBy(_.orderId),orderPayPattern)

   /*
   //最简单的写法：def,{},返回值都可以省略，此方法在spark编程中经常使用
   val timeoutFunction = (map: Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[OrderEvent]) => {
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }*/

/*    def timeoutFunction (map: Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[OrderEvent]) ={
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }*/

  /*
  //最简单的写法：def,{},返回值都可以省略，此方法在spark编程中经常使用
  val selectFunction = (map:Map[String,Iterable[OrderEvent]],out: Collector[OrderEvent]) =>{

    }*/
    /*def selectFunction(map:Map[String,Iterable[OrderEvent]],out: Collector[OrderEvent]): Unit ={

    }*/

    //val timeoutOrder = patternStream.flatSelect(orderTimeOutPut)(timeoutFunction)(selectFunction)

    val timeoutOrder = patternStream.flatSelect(orderTimeOutPut)((map: Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[OrderEvent]) => {
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    })((map:Map[String,Iterable[OrderEvent]],out: Collector[OrderEvent]) =>{

    })

    timeoutOrder.getSideOutput(orderTimeOutPut).print()
    env.execute()
  }

}
