package com.atguigu.flink.project.LoginFailWithoutCEP

import com.atguigu.flink.project.util.LoginEvent
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//5s内连续两次登陆失败
object LoginWithoutCEPExample {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements(
         LoginEvent("1", "0.0.0.0", "fail", "1"),
       // LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
    )
      .assignAscendingTimestamps(_.time.toLong * 1000)
      .keyBy(_.userID)
      .process(new MatcFunction)

    stream.print()
    env.execute()

  }


  class MatcFunction extends KeyedProcessFunction[String,LoginEvent,String] {

    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-fail",Types.of[LoginEvent])
    )

      lazy val timeStamp = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("ts",Types.of[Long])
      )

    override def processElement(value: LoginEvent, context: KeyedProcessFunction[String, LoginEvent, String]#Context, collector: Collector[String]): Unit = {

      if(value.behaive == "fail"){
        loginState.add(value)
        if(timeStamp.value() == 0L){
          timeStamp.update(value.time.toLong * 1000 + 5000L)//5s内连续登陆失败
          context.timerService().registerEventTimeTimer(value.time.toLong * 1000 + 5000L)//如果连续5s登陆失败，转到onTimer方法执行
        }
      }

      if(value.behaive == "success"){//如果5s内有一次登陆成功，则清空相关状态变量,删除定时寄存器
        loginState.clear()
        context.timerService().deleteEventTimeTimer(timeStamp.value())
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      var allLogins = ListBuffer[LoginEvent]()

      import  scala.collection.JavaConversions._
      for(login <- loginState.get){
        allLogins += login
      }
      loginState.clear()

      if(allLogins.size > 1){
        out.collect("5s内连续两次登陆失败")
      }
    }
  }

}
