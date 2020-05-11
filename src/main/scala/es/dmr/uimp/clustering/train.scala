package es.dmr.uimp.clustering

import java.io.File

import es.dmr.uimp.clustering.Clustering.elbowSelection
import org.apache.commons.io.FileUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Directory


object KMeansClusterInvoices {

  def main(args: Array[String]) {

    // Getting args
    val file = args(0)
    val pathModelKmeans = args(1)
    val pathThresholsKmeans = args(2)
    val pathModelBisect = args(3)
    val pathThresholsBisect = args(4)

    // Cleaning paths
    val paths = Array(pathModelKmeans, pathThresholsKmeans, pathModelBisect, pathThresholsBisect)
    for (dir <- paths){
      if (scala.reflect.io.File(dir).exists) {
        FileUtils.cleanDirectory( new File(dir))
      } else {
        new File(dir).mkdir();
      }
    }



    import Clustering._

    val sparkConf = new SparkConf().setAppName("ClusterInvoices").set("spark.sql.shuffle.partitions", "2")
    val sc = new SparkContext(sparkConf)


    // load data
    val df = loadData(sc, file)
    //println(df.printSchema)
    //val df_filtered = df.filter(df("hour") === "-1.0")
    //println(df_filtered.show(10))


    // Filter not valid entries
    val filtered = filterData(df)


   // Very simple feature extraction from an invoice
    val featurized = featurizeData(filtered)


    // Transform in a dataset for MLlib
    val dataset = toDataset(featurized)


    // We are going to use this a lot (cache it)
    dataset.cache()


    // Print a sample
    //dataset.take(5).foreach(println)
    println(dataset.count)


    // Training models
    val modelKmeans = trainModelKMeans(dataset)
    val modelBisect = trainModelBisect(dataset)


    // Save model
    modelKmeans.save(sc, pathModelKmeans)
    modelBisect.save(sc, pathModelBisect)



    // Save threshold
    val distancesKmeans = dataset.map(d => distToCentroid(d, modelKmeans))
    val thresholdKmeans = distancesKmeans.top(2000).last // set the last of the furthest 2000 data points as the threshold
    saveThreshold(thresholdKmeans, pathThresholsKmeans + "/threshold.txt")

    val distancesBisect = dataset.map(d => distToCentroidBisect(d, modelBisect))
    val thresholdBisect = distancesBisect.top(2000).last // set the last of the furthest 2000 data points as the threshold
    saveThreshold(thresholdBisect, pathThresholsBisect + "/threshold.txt")
  }

  /**
   * Train a KMean model using invoice data.
   */
  def trainModelKMeans( data: RDD[Vector]): KMeansModel = {

    val models = 1 to 20 map { k =>
      val kmeans = new KMeans()
      kmeans.setK(k) // find that one center
      kmeans.run(data)
    }

    val costs = models.map(model_ => model_.computeCost(data))
    val selected = elbowSelection(costs, 0.7)
    println("Selecting model: " + models(selected).k)
    models(selected)
  }


  def trainModelBisect(data: RDD[Vector]): BisectingKMeansModel = {

    val models = 1 to 20 map { k =>
      val bisect = new BisectingKMeans()
      bisect.setK(k) // find that one center
      bisect.run(data)
    }

    val costs = models.map(model_ => model_.computeCost(data))
    val selected = elbowSelection(costs, 0.7)

    System.out.println("Selecting model: " + models(selected).k)
    models(selected)
  }

  /**
   * Calculate distance between data point to centroid.
   */
  def distToCentroid(datum: Vector, model: KMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

  def distToCentroidBisect(datum: Vector, model: BisectingKMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }
}

