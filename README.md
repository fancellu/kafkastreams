# Kafkastreams

Kafka streams Scala example, using docker

## To start up Kafka

cd docker

`docker-compose up -d`

## To view the Kafdrop UI against our docker kafka

Goto http://localhost:9000/

For more info https://github.com/obsidiandynamics/kafdrop

## Create a topic

`docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic quickstart`

## Write to topic from stdin

`docker exec --interactive --tty broker kafka-console-producer --bootstrap-server broker:9092 --topic quickstart`

ctrl-d to end

## Read from topic

`docker exec --interactive --tty broker kafka-console-consumer --bootstrap-server broker:9092 --topic quickstart --from-beginning`

ctrl-c to stop reading, if you write messages from another terminal, you will see them arrive here

You could also use ProducerApp and ConsumerApp below with "quickstart" as first param

## Extra topics for KafkaStreamsApp example

`docker exec -it broker bash`

Then paste in the following

```
kafka-topics --bootstrap-server localhost:9092 --topic orders-by-user --create

kafka-topics --bootstrap-server localhost:9092 --topic discount-profiles-by-user --create --config "cleanup.policy=compact"

kafka-topics --bootstrap-server localhost:9092 --topic discounts --create --config "cleanup.policy=compact"

kafka-topics --bootstrap-server localhost:9092 --topic orders --create

kafka-topics --bootstrap-server localhost:9092 --topic payments --create

kafka-topics --bootstrap-server localhost:9092 --topic paid-orders --create

kafka-topics --bootstrap-server localhost:9092 --topic paid-order-total --create --config "cleanup.policy=compact"


```

Then type

`exit`

## To send data to various KafkaStreamsApp topics

You can run `KafkaStreamsProducerApp` or do it manually yourself as below

`docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --property parse.key=true --property key.separator=,  --topic discounts`

profile1,{"profile":"profile1","amount":0.5 }
profile2,{"profile":"profile2","amount":0.25 }
profile3,{"profile":"profile3","amount":0.15 }

`docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --property parse.key=true --property key.separator=,  --topic discount-profiles-by-user`

Daniel,profile1
Riccardo,profile2

`docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --property parse.key=true --property key.separator=,  --topic orders-by-user`

Daniel,{"orderId":"order1","user":"Daniel","products":[ "iPhone 13","MacBook Pro 15"],"amount":4000.0 }
Riccardo,{"orderId":"order2","user":"Riccardo","products":["iPhone 11"],"amount":800.0}

`docker exec -it broker kafka-console-producer --bootstrap-server broker:9092 --property parse.key=true --property key.separator=,  --topic payments`

order1,{"orderId":"order1","status":"PAID"}
order2,{"orderId":"order2","status":"PENDING"}

`docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic paid-orders --from-beginning`

Or use ConsumerApp below

## ComsumerApp and ProducerApp

These consume on paid-orders and produce to paid-orders, as basic examples of how to do this with the kafka non stream api

You can also give it another topic to target as its first param, e.g. quickstart 

## To stop Kafka

`docker-compose down`

## https://kafka.apache.org/documentation/streams/