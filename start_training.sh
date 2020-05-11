#!/bin/bash

FILE=./src/main/resources/training.csv
KMEANS_PATH=./clustering/
KMEANS_THRESHOLD=./threshold/
BISECT_PATH=./clustering_bisect/
BISECT_THRESHOLD=./threshold_bisect/
spark-submit --class es.dmr.uimp.clustering.KMeansClusterInvoices --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar \
${FILE} ${KMEANS_PATH} ${KMEANS_THRESHOLD} ${BISECT_PATH} ${BISECT_THRESHOLD}
