package com.atguigu.flink





import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  case class WordCountWithCount(name: String, count: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readStream = env.socketTextStream("hadoop102", 9999, '\n')

    readStream.flatMap(_.split(("\\W+")))
      .map(word => WordCountWithCount(word, 1))
      .keyBy("name")
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()

    env.execute("Wordcount")

  }
}