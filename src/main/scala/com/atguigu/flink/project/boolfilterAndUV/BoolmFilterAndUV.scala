package com.atguigu.flink.project.boolfilterAndUV

import com.atguigu.flink.project.util.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object BoolmFilterAndUV {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("hadoop102",9999,'\n')

    stream.map(line =>{
      val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(u => ("uv",u.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .trigger(new MyTrigger)
      .process(new MyUvWindowProcess)
      .print()

    env.execute()
  }



  /*onElement()添加到每个窗口的元素都会调用此方法。
  onEventTime()当注册的事件时间计时器触发时，将调用此方法。
  onProcessingTime()当注册的处理时间计时器触发时，将调用此方法。
  onMerge()与有状态触发器相关，并在两个触发器对应的窗口合并时合并它们的状态，例如在使用会话窗口时。(目前没使用过，了解不多)
  clear()执行删除相应窗口时所需的任何操作。(一般是删除定义的状态、定时器等)*/


  class MyTrigger  extends  Trigger[(String,Long),TimeWindow]{

    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      triggerContext.registerEventTimeTimer(w.getEnd)//注册定时器，定时时间到了调用(触发)onEventTime这个方法
      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(timer: Long, window: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
     //timer为定时器的当前时间
      if(timer == window.getEnd){
        val jedis = new Jedis("hadoop102",6379)
        val key = window.getEnd.toString
        //jedis.hget:返回哈希表UvCountHashTable中给定域key的值
        //窗口关闭在此打印窗口信息，而不是在windowProcess里面
        println((key, jedis.hget("UvCountHashTable", key)))
        TriggerResult.FIRE_AND_PURGE
      }
      TriggerResult.CONTINUE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

    }
  }

  class MyUvWindowProcess extends  ProcessWindowFunction[(String,Long),String,String,TimeWindow]{

    lazy val jedis = new Jedis("hadoop102",6379)
    lazy val bloom = new Bloom(1 << 16)
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

      val storeKey = context.window.getEnd.toString
      var  count = 0L

      //UV的数量保存在redis的表名为UvCountHashTable的storeKey字段：storeKey字段为窗口关闭时间
      if(jedis.hget("UvCountHashTable",storeKey) != null){
        count = jedis.hget("UvCountHashTable",storeKey).toLong
      }

      //为什么elements只有一个元素，
      // 是因为触发器的onElement方法对窗口里面每来一条数据就触发窗口执行，并且情况窗口元素
      //即触发器的onElement方法执行以后调用onProcess方法
      val userId = elements.last._2
      //offset是位数组的下标值
      val offset =  bloom.hash(userId.toString)

      // 这里为什么不用创建位数组就直接可以getbit呢？
      // 如果没有位数组的话，getbit会自动创建位数组，创建一个offset大小的位数组
      // 这里创建位数组的操作其实有点消耗性能
      // 所以优化策略是：提前针对窗口结束时间创建大的位数组
      // getbit 操作返回 false 的话，说明 userId 没来过
      val isExist = jedis.getbit(storeKey,offset)
      if(! isExist){//如果某一个userid对应的位数组不存在，相应位数组的相应位置1，并且redis相应的UvCountHashTable的storekey字段的userid数加1
        jedis.setbit(storeKey,offset,true)
        jedis.hset("UvCountHashTable",storeKey,(count + 1).toString)
      }

    }
  }

  //布隆是一个hash函数
class Bloom(size: Int) extends Serializable{

  //value的值是userId.toString
  //返回值是userID的对应位数组的下标值
  def hash(value :String): Long ={
    var result = 0
    for(i <- 0 until value.length){
      result = result * 61 + value.charAt(i)
    }
    (size - 1) & result
  }

}

}
