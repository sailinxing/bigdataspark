package com.bigdata.urlcount

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取出学科点击前三的
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd = sc.textFile("D:\\testsparkdata\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    }
    )
    val rdd1 = rdd.reduceByKey(_ + _)
    val rdd2 = rdd1.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    }
    )
    val rdd3 = rdd2.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3)
    })
    println(rdd3.collect().toBuffer)
    sc.stop()
  }
}
