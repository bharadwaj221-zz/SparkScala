
package com.visa.dpd.mai.mrm

import scala.collection.mutable
import org.apache.spark.mllib.fpm._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


//  spark2-submit --master yarn --conf spark.eventLog.enabled=false --num-executors 50 --executor-memory 10G --driver-memory 10G  --class com.visa.dpd.mai.mrm.KMeansClustering mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/hive/mpaa.db/cust_mrch_dtl/* 10 100

object KMeansClustering {
  def main(args: Array[String]) {

    val appName:java.lang.String = "KMeans"
    val conf = new SparkConf().setAppName(appName).setMaster(args(0))
    val sc = new SparkContext(conf)
    var inputPath = args(1)
    var numClusters = 50
    var numIterations = 1000
    inputPath = "/hive/mpaa.db/"
    numClusters = args(2).toInt
    numIterations = args(3).toInt
    val initialSet = mutable.HashSet.empty[(Int,Double)]
    val addToSet = (s: mutable.HashSet[(Int,Double)], v:(Int,Double)) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[(Int,Double)], p2: mutable.HashSet[(Int,Double)]) => p1 ++= p2
    val data=sc.textFile("aaa")
//    val mrchCnt = data.map(x => x.split("\001")(1).toInt).max+1
    val mrchCnt=5325
    val mappedData = data.map(x => (x.split("\001")(0).toInt,(x.split("\001")(1).trim.toInt,1.0)))
//    val mappedData = data.map(x => (x.split("\001")(0).trim.toInt,(x.split("\001")(1).trim.toInt,x.split("\001")(1).trim.toDouble)))
//    val mappedData = data.map(x => (x.split("\001")(0).trim.toInt,(x.split("\001")(1).trim.toInt,x.split("\001")(2).trim.toDouble)))
    val aggs = mappedData.aggregateByKey(initialSet)(addToSet,mergePartitionSets)
    val ratings = aggs.map(x => (x._1,Vectors.sparse(mrchCnt, x._2.toSeq))).cache()

    val clusters = KMeans.train(ratings.map(x => x._2), numClusters, numIterations)
    clusters.save(sc, "ClusterModel_"+numClusters+"clusters")
    val results = clusters.predict(ratings.map(x => x._2))
    val clusterAssignment = ratings.zip(results).map(x => x._1._1+"\t"+x._2)
    clusterAssignment.saveAsTextFile("ClusterAssignment_"+numClusters+"clusters_"+numIterations+"iters")






  }
}