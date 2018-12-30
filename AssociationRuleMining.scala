
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


  //  spark2-submit --master yarn --conf spark.eventLog.enabled=false --num-executors 50 --executor-memory 10G --driver-memory 10G  --class com.visa.dpd.mai.mrm.AssociationRuleMining mrm-0.0.1-SNAPSHOT.jar yarn hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_xb_mrch_dtl/usr_cls=Accelerate/*/xb=1 NULL /user/bjayaram 0.0001 0.75

object AssociationRuleMining {
  def main(args: Array[String]) {

    val appName:java.lang.String = "AssociationRuleMining"
    val conf = new SparkConf().setAppName(appName).setMaster(args(0))
    val sc = new SparkContext(conf)

    val inputPath =  args(1)
//    val mappingPath = args(2)
    val support = args(3).toDouble
    val confidence = args(4).toDouble
    val outputPath = args(2)+"/Sup="+support+"_Conf="+confidence
    val statusStr = " ********** "

    val fs = FileSystem.get(new Configuration())
    val data = sc.textFile(inputPath,minPartitions = 10000)



    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    println(statusStr+"Read data"+statusStr)

    val tranSets = data.map(x => x.split("\t")).map(x => ((x(4).replace("-","").substring(0,6).toInt,x(0).toInt), x(1))).aggregateByKey(initialSet)(addToSet,mergePartitionSets)
    //tranSets.map(x => x._2.toList).foreach(println)
    val transactions = tranSets.map(x => x._2.toArray).cache()
    println(statusStr+"No. of item sets = "+transactions.count()+statusStr)
    var targetPath = new Path(outputPath)
    if (fs.exists(targetPath))
      fs.delete(targetPath,true)
    println(statusStr+"Output directory = "+outputPath+statusStr)
    val numPartitions = 100
    var fpg = new FPGrowth().setMinSupport(support).setNumPartitions(numPartitions)

    var model = fpg.run(transactions);
    println(statusStr+"Generated FP Growth rules"+statusStr)
    var rules = model.generateAssociationRules(confidence)
    rules.map(rule => rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence).saveAsTextFile(outputPath+"/FPGrowth")
    println(statusStr+"Saving FP Growth rules"+statusStr)
    val ar = new AssociationRules().setMinConfidence(confidence)
//    val results = ar.run(model.freqItemsets)
//    println(statusStr+"Generating association rules"+statusStr)
//
//    results.map(rule => rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent.mkString("[", ",", "]") + ", " + rule.confidence).saveAsTextFile(outputPath+"/AssociationRules")
//    println(statusStr+"Saving association rules"+statusStr)


  }
}