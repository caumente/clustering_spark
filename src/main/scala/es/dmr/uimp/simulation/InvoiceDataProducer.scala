package es.dmr.uimp.simulation

import java.util.{Properties}
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random

object InvoiceDataProducer extends App {

  // Parameters
  val events = args(0)
  val topic = args(1)
  val brokers = args(2)
  //val events = "./src/main/resources/production_test.csv"
  //val test = new File(events)
  //test.getAbsolutePath()
  //val topic = "purchases"
  //val brokers = "localhost:9092"

  println("Los parametros con los que se inicializa el script son:")
  println("Los datos que leeremos se encuentran en: " + events)
  println("Se enviaran al topic de Kafka: " + topic)
  println("Concretamente al broker: " + brokers)

  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  println("Instanciamos el Kafka producer: ")
  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  println("Sending purchases!")

  // Read events from file and push them to kafka
  //for (line <- Source.fromFile(args(0)).getLines()) {
  for (line <- Source.fromFile(events).getLines()) {
      // Get invoice id
      val invoiceNo = line.split(",")(0)
      val data = new ProducerRecord[String, String](topic, invoiceNo, line)

      println("El producer genera los siguientes datos:" + data)
      producer.send(data)

      // Introduce random delay
      Thread.sleep(5 +  (5*Random.nextFloat()).toInt)
  }

  producer.close()
}