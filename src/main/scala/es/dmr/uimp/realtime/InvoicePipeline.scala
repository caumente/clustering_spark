package es.dmr.uimp.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import es.dmr.uimp.clustering.KMeansClusterInvoices
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  def filterPurchase(purchase: Purchase): Boolean = {
    purchase.invoiceNo == null || purchase.invoiceDate == null ||
    purchase.quantity.isNaN || purchase.unitPrice.isNaN ||
    purchase.unitPrice < 0 || purchase.country == null ||
    purchase.customerID == null
  }


  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)



  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(40))

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast
    val KMeansData = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val KMeansModel: Broadcast[KMeansModel] = sc.broadcast(KMeansData._1)
    val KMeansTh: Broadcast[Double] = sc.broadcast(KMeansData._2)


    val KMeansBisectData = loadBisectAndThreshold(sc, modelFileBisect, thresholdFileBisect)
    val KMeansBisectModel: Broadcast[BisectingKMeansModel] = sc.broadcast(KMeansBisectData._1)
    val KMeansBisectTh: Broadcast[Double] = sc.broadcast(KMeansBisectData._2)

    val anomalizer = isAnomaly(KMeansModel, KMeansBisectModel, KMeansTh, KMeansBisectTh)(_,_)

    // TODO: Build pipeline



    // connect to kafka
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)
    val purchasesStream = getPurchasesStream(purchasesFeed)

    val invoices: DStream[(String, Invoice)] = purchasesStream
        .filter(data => !filterPurchase(data._2))
        .window(Seconds(40), Seconds(1))
        .updateStateByKey(updateFunction)
    // TODO: rest of pipeline
    detectAnomaly(sc)(invoices, "kmeans", brokers)(anomalizer)
    detectAnomaly(sc)(invoices, "bisect", brokers)(anomalizer)

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
  val ANOMALY_KMEANS="anomaly_kmeans"
  val ANOMALY_BISECT="anomaly_bisect"

  def detectAnomaly(sc: SparkContext)
                   (invoices: DStream[(String, Invoice)], model: String, broker: String)
                   (anomalizer: (Invoice, String) => Boolean) = {
    def sendInvoices(topic: String) = {
      invoices.filter{data => anomalizer(data._2,model) == true}
        .transform{rdd => rdd.map(data => (data._1.toString, data._2.toString))}
        .foreachRDD{rdd => publishToKafka(topic)(sc.broadcast(broker))(rdd)}
    }

    if (model == "kmeans")  {
      sendInvoices(ANOMALY_KMEANS)
    }
    else if (model == "bisect"){
      sendInvoices(ANOMALY_BISECT)
    } else {
      throw new Exception("need model")
    }
  }

  def updateFunction(newValues: Seq[Purchase], state: Option[Invoice]): Option[Invoice] = {
    val invoiceNo = newValues.head.invoiceNo
    val unitPrices = newValues.map(purchase => purchase.unitPrice)

    val avgUnitPrice = unitPrices.sum / unitPrices.length
    val minUnitPrice = unitPrices.min
    val maxUnitPrice = unitPrices.max
    val time = newValues.head.invoiceDate.split(" ")(1).split(":")(0).toDouble
    val numberItems = newValues.map(purchase => purchase.quantity).sum
    val lines = newValues.length
    val lastUpdated = 0
    val customer = newValues.head.customerID

    Some(Invoice(invoiceNo, avgUnitPrice, minUnitPrice, maxUnitPrice, time, numberItems, lastUpdated, lines, customer))
  }

  val CANCELLATION_TOPIC = "cancelaciones"
  def detectCancelation(sc: SparkContext)(purchasesStream: DStream[(String, Purchase)], brokers: String): Unit= {
    purchasesStream.filter(data => data._2.invoiceNo.startsWith("C"))
      .countByWindow(Minutes(8), Minutes(1))
      .transform{rdd => rdd.map(c => (c.toString,c.toString))}
      .foreachRDD{rdd => publishToKafka(CANCELLATION_TOPIC)(sc.broadcast(brokers))(rdd)}
  }

  val BAD_PURCHASES_TOPIC = "facturas_erroneas"
  def detectBadPurchases(sc: SparkContext)(purchasesStream: DStream[(String, Purchase)], brokers: String): Unit= {
    purchasesStream.filter(data => filterPurchase(data._2))
      .transform{rdd => rdd.map(purc => (purc._1.toString, purc._2.toString))}
      .foreachRDD{rdd => publishToKafka(BAD_PURCHASES_TOPIC)(sc.broadcast(brokers))(rdd)}
  }


  def isAnomaly(kMeansModel : Broadcast[KMeansModel], kMeansBisectModel: Broadcast[BisectingKMeansModel],
                kMeansThreshold: Broadcast[Double], kMeansBisectThreshold: Broadcast[Double])
               (invoice: Invoice, modelType: String): Boolean ={

    val features = ArrayBuffer[Double]()
      features.append(invoice.avgUnitPrice)
      features.append(invoice.minUnitPrice)
      features.append(invoice.maxUnitPrice)
      features.append(invoice.time)
      features.append(invoice.numberItems)


    val featuresVector = Vectors.dense(features.toArray)
    if( modelType == "kmeans"){
      val distanceKMeans = KMeansClusterInvoices.distToCentroid(featuresVector, kMeansModel.value)
      distanceKMeans> kMeansThreshold.value
    } else if (modelType == "bisect"){
      val distanceBisect = KMeansClusterInvoices.distToCentroidBisect(featuresVector, kMeansBisectModel.value)
      distanceBisect>kMeansBisectThreshold.value
    } else{
      throw new Exception("need model type")
    }
  }


    def publishToKafka(topic : String)(kafkaBrokers : Broadcast[String])(rdd : RDD[(String, String)]) = {
    rdd.foreachPartition( partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach( record => {
        producer.send(new ProducerRecord[String, String](topic, record._1,  record._2.toString))
      })
      producer.close()
    })
  }

  def kafkaConf(brokers : String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
    * Load the model information: centroid and threshold
    */
  def loadKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[KMeansModel,Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans, threshold)
  }

  def loadBisectAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[BisectingKMeansModel,Double] = {
    val bisect = BisectingKMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (bisect, threshold)
  }


  def connectToPurchases(ssc: StreamingContext, zkQuorum : String, group : String,
                         topics : String, numThreads : String): DStream[(String, String)] ={

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  def parsePurchase(purchase: String): Purchase = {
    val csvParserSettings = new CsvParserSettings()
    csvParserSettings.detectFormatAutomatically()
    val csvParser = new CsvParser(csvParserSettings)
    val parsedPurchase = csvParser.parseRecord(purchase)

    parsedToPurchase(parsedPurchase)
  }

  def parsedToPurchase(record: Record): Purchase =
    Purchase(
      record.getString(0),
      record.getInt(3),
      record.getString(4),
      record.getDouble(5),
      record.getString(6),
      record.getString(7)
    )
  
  def getPurchasesStream(feed: DStream[(String, String)]): DStream[(String, Purchase)] = {
    feed.transform { rdd => rdd.map {
      input =>
        val invoiceId = input._1
        val purchase = parsePurchase(input._2)

        (invoiceId, purchase)
    }
    }
  }
}
