package com.felstar.ks

import com.felstar.ks.KafkaStreamsApp.Topics._
import org.apache.kafka.streams.StreamsConfig

import scala.util.Random

object KafkaStreamsProducerApp extends App{

  import org.apache.kafka.clients.producer._

  import java.util.Properties

  val  props = new Properties()
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  {
    val profile1 = new ProducerRecord(DiscountsTopic, "profile1",
      s"""{
         |"profile":"profile1",
         |"amount":0.5
         |}""".stripMargin)

    val profile2 = new ProducerRecord(DiscountsTopic, "profile2",
      s"""{
         |"profile":"profile1",
         |"amount":0.25
         |}""".stripMargin)

    val profile3 = new ProducerRecord(DiscountsTopic, "profile3",
      s"""{
         |"profile":"profile3",
         |"amount":0.15
         |}""".stripMargin)


    producer.send(profile1)
    producer.send(profile2)
    producer.send(profile3)
  }

  {
    val discount1=new ProducerRecord(DiscountProfilesByUserTopic, "Daniel","profile1")
    producer.send(discount1)
    val discount2=new ProducerRecord(DiscountProfilesByUserTopic, "Riccardo","profile2")
    producer.send(discount2)
  }

  def uuid = java.util.UUID.randomUUID.toString

  val order1ID= s"order$uuid"
  val order2ID= s"order$uuid"

  {
    val order1 = new ProducerRecord(OrdersByUserTopic, "Daniel",
      s"""{"orderId":"$order1ID","user":"Daniel","products":[ "iPhone 133","MacBook Pro 19","${Random.alphanumeric.take(10).mkString}"],"amount":4200.0 }""")

    val order2 = new ProducerRecord(OrdersByUserTopic, "Riccardo",
      s"""{"orderId":"$order2ID","user":"Riccardo","products":["iPhone 111","${Random.alphanumeric.take(5).mkString}"],"amount":804.0}""")

    producer.send(order1)
    producer.send(order2)
  }

  {
    val payment1 = new ProducerRecord(PaymentsTopic, "order1",
      s"""{"orderId":"$order1ID","status":"PAID"}""")

    val payment2 = new ProducerRecord(PaymentsTopic, "order2",
      s"""{"orderId":"$order2ID","status":"PENDING"}""")
    producer.send(payment1)
    producer.send(payment2)
  }

  producer.close()
}
