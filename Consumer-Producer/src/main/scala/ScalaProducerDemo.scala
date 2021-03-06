import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ScalaProducerDemo extends App {

  val events = 10000
  val topic = "test_topic"
  val brokers = "localhost:9092"

  val rnd = new Random()
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerDemo")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  for(nEvents <- Range(0, events)) {
    val runtime = new Date().getTime()
    val ip = "192.168.2." + rnd.nextInt(255)
    val msg = runtime + ", " + nEvents + "www.example.com ," + ip
    val data = new ProducerRecord[String, String](topic, ip, msg)

    //async
    //producer.send(data, (m,e) => {})
    //sync

    producer.send(data)
  }

  System.out.println("send per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
