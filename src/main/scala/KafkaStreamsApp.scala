package com.felstar.ks

import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._

object KafkaStreamsApp {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double) // in percentage points

    case class Payment(orderId: OrderId, status: String)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
    // final val PaidOrderTotalTopic = "paid-order-total"
  }

  import Domain._
  import Topics._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => {
      if (a == null) {
        "".getBytes
      } else a.asJson.noSpaces.getBytes
    }
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"Error converting the message $aOrError, $error for $aAsString")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  def main(args: Array[String]): Unit = {

    val builder = new StreamsBuilder

    val paidOrders: KStream[OrderId, Order] = {

      val userProfilesTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)
      val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)
      val paymentsStream: KStream[OrderId, Payment] = builder.stream[OrderId, Payment](PaymentsTopic)
      val usersOrdersStreams: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

      val ordersWithUserProfileStream: KStream[UserId, (Order, Profile)] =
        usersOrdersStreams.join(userProfilesTable) { (order, profile) =>
          (order, profile)
        }

      val discountedOrdersStream: KStream[UserId, Order] =
        ordersWithUserProfileStream.join[Profile, Discount, Order](discountProfilesGTable)(
          { case (_userId, (_order, profile)) => profile }, // Joining key
          { case ((order, _profile), discount) => order.copy(amount = order.amount * discount.amount) }
        )

      val ordersStream: KStream[OrderId, Order] = discountedOrdersStream.selectKey { (_, order) => order.orderId }

      val joinOrdersAndPayments: (Order, Payment) => Option[Order] = (order: Order, payment: Payment) =>
        if (payment.status == "PAID") Option(order) else Option.empty[Order]

      val joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.of(15, ChronoUnit.MINUTES))

      ordersStream.join[Payment, Option[Order]](paymentsStream)(joinOrdersAndPayments, joinWindow)
        .flatMapValues {
          _.toList
        }
    }

    paidOrders.to(PaidOrdersTopic)

    val topology = builder.build()

    val props = new Properties

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

    println(topology.describe())

    val application: KafkaStreams = new KafkaStreams(topology, props)

    application.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler {
      override def handle(exception: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
        println(s"Got an exception $exception")
        Thread.sleep(1000)
        StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
      }
    })

    application.start()

    println("started ")

  }

}
