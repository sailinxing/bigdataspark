package com.bigdata.urlcount

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("D:\\testsparkdata\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    }
    )
    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    }).cache()
    //cache会将数据缓存到内存中，其是一个transfamation
    val insts = rdd3.map(_._1).distinct().collect()
    val hostParitioner = new HostParitioner(insts)
    rdd3.partitionBy(hostParitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    }).saveAsTextFile("D://testsparkdata//out4")
    //    rdd3.repartition(3).saveAsTextFile("D://testsparkdata//out1")
  }
}

class HostParitioner(insts: Array[String]) extends Partitioner {
  val parMap = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- insts) {
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions = insts.length

  override def getPartition(key: Any) = {
    parMap.getOrElse(key.toString, 0)
  }
}
