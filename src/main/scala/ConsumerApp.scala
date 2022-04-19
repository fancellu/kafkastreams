package com.felstar.ks

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.streams.StreamsConfig

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

/** To test this, try sending to this topic, e.g. ProducerApp or the below
 * docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --property parse.key=true --property key.separator=, --topic paid-orders
 * 1,hello
 * 2,byebye */

object ConsumerApp extends App {

  import java.util.Properties

  val TOPIC = if (args.length==1) args(0) else "paid-orders"

  val props = new Properties()
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(List(TOPIC).asJavaCollection)

  println(s"consuming $TOPIC")

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(10.seconds.toJava)
    for (record <- records.asScala) {
      println(record)
    }
  }

}
