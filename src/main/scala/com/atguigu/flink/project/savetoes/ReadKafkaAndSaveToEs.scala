package com.atguigu.flink.project.savetoes

import java.sql.Timestamp
import java.util
import java.util.Properties

import com.atguigu.flink.project.util.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import java.util.ArrayList

import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.client.{Request, Requests}

import scala.collection.mutable.ListBuffer

object ReadKafkaAndSaveToEs {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    var props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    props.setProperty("group.id", "consumer-group")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("userbeh",new SimpleStringSchema(),props))
   .map(line => {
      val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
    }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000
      }
    )
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      .aggregate(new MyAggFun,new MyProcessWindowe)
      .keyBy(_.windowEnd)
      .process(new MyKeyedProcessFunTop(3))
      stream.print()

    val httHosts: util.ArrayList[HttpHost] = new ArrayList[HttpHost]()
    httHosts.add(new HttpHost("hadoop102",9200,"http"))

    val esSinkBuilder: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](
      httHosts,
      new ElasticsearchSinkFunction[String]{

        /*def createIndexRequest(element:String) = {
          val json = new java.util.HashMap[String,String]
          json.put("myUserbe",element)

          Requests.indexRequest()
            .index("user-behavior")
            .`type`("_doc")
            .source(json)
        }*/

        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          //requestIndexer.add(createIndexRequest(t))
          val json = new java.util.HashMap[String,String]
          json.put("myUserbe",t)

          val request = Requests.indexRequest()
            .index("user-behavior")
            .`type`("_doc")
            .source(json)
          requestIndexer.add(request)

        }
      }
    )

   esSinkBuilder.setBulkFlushMaxActions(1)
    stream.addSink(esSinkBuilder.build())

    env.execute()
  }

  class MyAggFun extends  AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class MyProcessWindowe extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.iterator.next()))
    }
  }

  class MyKeyedProcessFunTop(i: Int) extends  KeyedProcessFunction[Long,ItemViewCount,String]{

    lazy val listItem: ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("listItem",Types.of[ItemViewCount])
    )

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

      listItem.add(i)
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

      val listItemBuffer: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()

      import scala.collection.JavaConversions._
      for(item <- listItem.get){
        listItemBuffer += item
      }
      listItem.clear()

      val listTopIResult: ListBuffer[ItemViewCount] = listItemBuffer.sortBy(-_.count).take(i)

      var result= new StringBuilder

      result.append("================================")
        .append("时间：")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for(i <- listTopIResult.indices){
        val currentItemView = listTopIResult(i)
        result.append("No")
          .append(i + 1)
          .append(":")
          .append("商品ID = ")
          .append(currentItemView.itemId)
          .append("\n")
          .append("点击量 = ")
          .append(currentItemView.count)
          .append("\n")
      }
      result.append("================================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())

    }

  }

}
