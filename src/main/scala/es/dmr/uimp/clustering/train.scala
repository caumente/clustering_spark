package es.dmr.uimp.clustering

import es.dmr.uimp.clustering.Clustering.elbowSelection
import es.dmr.uimp.clustering.Clustering.elbowSelection2
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import scala.reflect.io.Directory
import java.io.File


object KMeansClusterInvoices {

  def main(args: Array[String]) {


    val file = args(0)
    val path_model = args(1)
    val path_threshold = args(2)


    val drop_path = new Directory(new File(path_model))
    drop_path.deleteRecursively()
    val drop_path_thres = new Directory(new File(path_threshold))
    drop_path_thres.deleteRecursively()
    //val file = "./src/main/resources/training_test.csv"
    //val path_model = "./clustering/"
    //val path_threshold = "./threshold/"


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


    // Print a sampl
    //dataset.take(5).foreach(println)
    println(dataset.count)


    val model = trainModel(dataset)
    val model2 = trainModel2(dataset)
    // Save model
    model.save(sc, path_model)
    model2.save(sc, path_model + "_2")


    // Save threshold
    val distances = dataset.map(d => distToCentroid(d, model))
    val threshold = distances.top(2000).last // set the last of the furthest 2000 data points as the threshold

    saveThreshold(threshold, path_threshold + "/threshold.txt")
  }

  /**
   * Train a KMean model using invoice data.
   */
  def trainModel(data: RDD[Vector]): KMeansModel = {

    val models = 1 to 20 map { k =>
      val kmeans = new KMeans()
      kmeans.setK(k) // find that one center
      kmeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection(costs, 0.7)
    System.out.println("Selecting model: " + models(selected).k)
    models(selected)
  }


  def trainModel2(data: RDD[Vector]): KMeansModel = {

    val models = 1 to 20 map { k =>
      val kmeans = new KMeans()
      kmeans.setK(k) // find that one center
      kmeans.run(data)
    }

    val costs = models.map(model => model.computeCost(data))

    val selected = elbowSelection2(costs, 0.7)
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

}

