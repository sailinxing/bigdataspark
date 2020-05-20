package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setJars(Array("D:\\mycode\\idea\\bigdataspark\\target\\bigdata-spark-1.0-SNAPSHOT.jar")).setMaster("spark://node3:7077")
    //非常重要，是通向Spark集群的入口
    val sc = new SparkContext(conf)
    //textFile产生两个rdd:hadoopRdd , mapPartitionsRdd
    sc.textFile("hdfs://node1:9000/sparkwc")
      // 产生一个:mapPartitionsRdd
      .flatMap(_.split(" "))
      // 产生一个:mapPartitionsRdd
      .map((_, 1))
      // 产生一个:shuffledRdd
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      // 产生一个:mapPartitionsRdd
      .saveAsTextFile("hdfs://node1:9000/sparkout5")
    sc.stop()
  }
}
