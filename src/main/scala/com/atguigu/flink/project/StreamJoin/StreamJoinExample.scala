package com.atguigu.flink.project.StreamJoin

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StreamJoinExample {

  //定义侧输出流保存对象
  val unmatchedOrders = new OutputTag[OrderEvent]("ummatchorderEvent") {}
  val ummatchPays = new OutputTag[PayEvent]("unmatchPays")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
   env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val orders = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("1", "pay", "1558430844"),
      OrderEvent("2", "pay", "1558430845"),
      OrderEvent("3", "create", "1558430849"),
      OrderEvent("3", "pay", "1558430849")
    ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val pays = env.fromCollection(List(
      PayEvent("1", "weixin", "1558430847"),
      PayEvent("2", "zhifubao", "1558430848"),
      PayEvent("4", "zhifubao", "1558430850")
    ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.orderId)

    val connectedStream: DataStream[(OrderEvent, PayEvent)] = orders.connect(pays)
      .process(new ConnectedProcessFunction)

    connectedStream.getSideOutput[OrderEvent](unmatchedOrders).print()
    connectedStream.getSideOutput[PayEvent](ummatchPays).print()

    env.execute()

  }

  class ConnectedProcessFunction extends  CoProcessFunction[OrderEvent,PayEvent,(OrderEvent,PayEvent)]{

    lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("myorderState",Types.of[OrderEvent])
    )

    lazy val payState: ValueState[PayEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("mypayState",Types.of[PayEvent])
    )
    override def processElement1(order: OrderEvent, context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context, collector: Collector[(OrderEvent, PayEvent)]): Unit = {

      val pay: PayEvent = payState.value()

      if(pay != null){

        collector.collect((order,pay))
        payState.clear()

      }else{
        orderState.update(order)

        context.timerService().registerEventTimeTimer(order.eventTime.toLong * 1000)
      }

    }

    override def processElement2(pay: PayEvent, context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context, collector: Collector[(OrderEvent, PayEvent)]): Unit = {

      val order: OrderEvent = orderState.value()
      if(order != null){
        collector.collect((order,pay))
        orderState.clear()
      }else{
        payState.update(pay)

        context.timerService().registerEventTimeTimer(pay.eventTime .toLong * 10000)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#OnTimerContext, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      //如果相同key对应的流1s没有到，定时器触发，该流放入到侧输出流中
      if(payState.value() != null){
        ctx.output(ummatchPays,payState.value())
        payState.clear()
      }

      if(orderState.value() != null){
        ctx.output(unmatchedOrders,orderState.value())
        orderState.clear()
      }

    }
  }

}
