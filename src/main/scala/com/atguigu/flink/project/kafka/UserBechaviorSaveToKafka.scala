package com.atguigu.flink.project.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object UserBechaviorSaveToKafka {

  def main(args: Array[String]): Unit = {

    var props: Properties = new Properties()
    props.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](props)


    val source: Iterator[String] = io.Source.fromFile("F:\\flink-study\\src\\main\\resources\\UserBehavior.csv").getLines()

    import scala.collection.JavaConversions._
    for(data <- source){

      producer.send(new ProducerRecord[String,String]("userbeh",data.toString))

    }

    producer.close()
  }

}
