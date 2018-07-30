import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutorService, Executors}

import kafka.utils.Logging
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

class ScalaConsumerDemo(val brokers: String,
                        val groupId: String,
                        val topic: String) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  val executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.close()

    if(executor!=null)
      executor.shutdown();
  }

  def createConsumerConfig(str: String, str1: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute( new Runnable {
      override def run(): Unit = {
        while(true){
          val records = consumer.poll(1000)

          for(record <- records.iterator()){
            System.out.println(s"Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
          }
        }
      }
    })
  }
}

object ScalaConsumerDemo extends App {

  val example = new ScalaConsumerDemo("localhost:9092" ,"group1","test_topic" )
  example.run()
}
