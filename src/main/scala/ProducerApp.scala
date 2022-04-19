package com.felstar.ks

import org.apache.kafka.streams.StreamsConfig

object ProducerApp extends App{

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = if (args.length==1) args(0) else "paid-orders"

  println(s"producing to $TOPIC")

  for(i<- 1 to 10){
    val record = new ProducerRecord(TOPIC, s"key$i", s"hello $i")
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "keyend", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()
}
