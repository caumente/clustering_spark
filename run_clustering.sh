#!/bin/bash

FILE=./src/main/resources/training.csv
SAVE=./clustering/
THRESHOLD=./threshold/
spark-submit --class es.dmr.uimp.clustering.KMeansClusterInvoices --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar \
${FILE} ${SAVE} ${THRESHOLD} 
