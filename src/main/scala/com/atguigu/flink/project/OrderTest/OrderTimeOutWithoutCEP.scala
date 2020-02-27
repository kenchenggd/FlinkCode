package com.atguigu.flink.project.OrderTest

import com.atguigu.flink.project.util.OrderEvent
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeOutWithoutCEP {

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
    )).assignAscendingTimestamps(_.ts.toLong * 1000)

    val orders = stream.keyBy(_.orderId)
      .process(new OrderMatchFunction)
      .print()

    env.execute()

  }

  class OrderMatchFunction extends KeyedProcessFunction[String,OrderEvent,String]{

    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order",Types.of[OrderEvent])
    )

    override def processElement(order: OrderEvent, context: KeyedProcessFunction[String, OrderEvent, String]#Context, collector: Collector[String]): Unit = {

      val timerService = context.timerService()
      //创建订单5s以后没有支付就认为是订单超时
      if(order.op == "create"){
        if(orderState.value() == null){
          orderState.update(order)
        }
        }else {
        orderState.update(order)
      }

      timerService.registerEventTimeTimer(order.ts.toLong * 1000 + 5 * 1000)
      }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {

      val savedOrder = orderState.value()

      if(savedOrder != null && (savedOrder.op == "create")){
        out.collect(savedOrder.toString)
      }
      orderState.clear()
    }
  }

}
