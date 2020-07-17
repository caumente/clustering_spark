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
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)



  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1)) //Seconds(20)

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast

    // Loading models and thresholds
    val KMeansData = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val KModel = ssc.sparkContext.broadcast(KMeansData._1)
    val KThres= ssc.sparkContext.broadcast(KMeansData._2)

    val KMeansBisectData = loadBisectAndThreshold(sc, modelFileBisect, thresholdFileBisect)
    val BModel: Broadcast[BisectingKMeansModel] = ssc.sparkContext.broadcast(KMeansBisectData._1)
    val BThreshold: Broadcast[Double] = ssc.sparkContext.broadcast(KMeansBisectData._2)

    // broadcasting variables
    val broadcastBrokers: Broadcast[String] = ssc.sparkContext.broadcast(brokers)


    //TODO: Build pipeline

    // connect to kafka
    val purchasesFeed : DStream[(String, String)] = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)
    val purchasesStream = getPurchasesStream(purchasesFeed)

    // Analyzing if a purchase is wrong or cancel
    detectCancellations(purchasesStream, broadcastBrokers)
    detectWrongPurchases(purchasesStream, broadcastBrokers)


    // TODO: rest of pipeline

    val invoices: DStream[(String, Invoice)] = purchasesStream
      .filter(data => !badPurchase(data._2))
      .window(Seconds(40), Seconds(40))
      .updateStateByKey(updateFunction)


    val anomalizer = isAnomaly(KModel, BModel, KThres, BThreshold)(_,_)
    detectAnomaly(invoices, "kmeans", broadcastBrokers)(anomalizer)
    detectAnomaly(invoices, "bisection", broadcastBrokers)(anomalizer)



    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  /**
   *
   * Old functions: Kafka connections
   *
   */
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

  def connectToPurchases(ssc: StreamingContext, zkQuorum : String, group : String,
                         topics : String, numThreads : String): DStream[(String, String)] ={

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  /**
   * Old functions: Load the model information: centroid and threshold
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



  /**
   *
   * New functions
   *
   */

  // Dealing with pruchases
  def getPurchasesStream(kafkaFeed: DStream[(String, String)]): DStream[(String, Purchase)] = {
    val purchasesStream = kafkaFeed.transform { inputRDD =>
      inputRDD.map { input =>
        val invoiceId = input._1
        val purchaseAsString = input._2

        val purchase = parsePurchase(purchaseAsString)

        (invoiceId, purchase)
      }
    }

    purchasesStream
  }

  def parsePurchase(purchase: String): Purchase = {

    val csvParserSettings = new CsvParserSettings()
    val csvParser = new CsvParser(csvParserSettings)
    val parsedPurchase = csvParser.parseRecord(purchase)

    //parsedToPurchase(parsedPurchase)
    Purchase(
      parsedPurchase.getString(0),
      parsedPurchase.getInt(3),
      parsedPurchase.getString(4),
      parsedPurchase.getDouble(5),
      parsedPurchase.getString(6),
      parsedPurchase.getString(7)
    )

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

  // Cancelled purchases
  val TOPIC_INVOICES_CANCELLED = "cancelledPurchases"
  def detectCancellations(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]): Unit = {
    purchasesStream
      .filter(data => data._2.invoiceNo.startsWith("C"))
      .countByWindow(Minutes(8), Minutes(1))
      .transform {invoicesTupleRDD =>
        invoicesTupleRDD.map(count => (count.toString, "The number of invoices cancelled in last 8 minutes are: " + count.toString))
      }
      .foreachRDD { rdd =>
        publishToKafka(TOPIC_INVOICES_CANCELLED)(broadcastBrokers)(rdd)
      }
  }

  // Wrong purchases
  val TOPIC_INVOICES_WRONG = "wrongPurchases"
  def detectWrongPurchases(purchasesStream: DStream[(String, Purchase)], broadcastBrokers: Broadcast[String]): Unit = {
    purchasesStream
      .filter(data => isWrongPurchase(data._2))
      .transform {rdd => rdd.map(purchase => (purchase._1 , "The invoice " + purchase._1 + " contains wrong purchases."))}
      .foreachRDD { rdd => publishToKafka(TOPIC_INVOICES_WRONG)(broadcastBrokers)(rdd)
      }
  }

  def isWrongPurchase(purchase: Purchase): Boolean = {

    (purchase.invoiceNo == null || purchase.invoiceDate == null || purchase.customerID == null ||
      purchase.invoiceNo.isEmpty || purchase.invoiceDate.isEmpty || purchase.customerID.isEmpty ||
      purchase.unitPrice.isNaN || purchase.quantity.isNaN || purchase.country.isEmpty ||
      purchase.unitPrice.<(0) ) && !purchase.invoiceNo.startsWith("C")
  }

  // Cancelled and wrong pruchases
  def badPurchase(purchase: Purchase): Boolean = {
    purchase.invoiceNo == null || purchase.invoiceNo.startsWith("C") ||
      purchase.invoiceDate == null || purchase.customerID == null ||
      purchase.quantity.isNaN || purchase.unitPrice.isNaN ||
      purchase.unitPrice < 0 || purchase.country == null
  }


  // Anomaly detection
  val ANOMALY_KMEANS="anomalyKmeans"
  val ANOMALY_BISECT="anomalyBisection"

  def detectAnomaly(invoices: DStream[(String, Invoice)], model: String, broadcastBrokers: Broadcast[String])
                   (anomalizer: (Invoice, String) => Boolean) = {

    def sendInvoices(topic: String) = {
      invoices.filter{data => anomalizer(data._2,model) == true}
        .transform{rdd => rdd.map(data => (data._1.toString, "Anomaly detected by " + model + " method into invoice number: " + data._1.toString))}
        .foreachRDD{rdd => publishToKafka(topic)(broadcastBrokers)(rdd)}
    }

    if (model == "kmeans")  {
      sendInvoices(ANOMALY_KMEANS)
    }
    else if (model == "bisection"){
      sendInvoices(ANOMALY_BISECT)
    } else {
      throw new Exception("Need a model: kmeans or bisection")
    }
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
    } else if (modelType == "bisection"){
      val distanceBisect = KMeansClusterInvoices.distToCentroidBisect(featuresVector, kMeansBisectModel.value)
      distanceBisect>kMeansBisectThreshold.value
    } else{
      throw new Exception("need model type")
    }
  }


}


