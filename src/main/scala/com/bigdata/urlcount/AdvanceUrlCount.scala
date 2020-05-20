package com.bigdata.urlcount

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object AdvanceUrlCount {
  def main(args: Array[String]): Unit = {
    //从数据库中加载规则
    val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")

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
    //    val rddjava=rdd2.filter(_._1=="java.itcast.cn")
    //    val sortJava=rddjava.sortBy(_._3,false).take(3)
    //    println(sortJava.toBuffer)
    for (ins <- arr) {
      val rdd3 = rdd2.filter(_._1 == ins)
      val result = rdd2.sortBy(_._3, false).take(3)
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数， 访问日期
      println(result.toBuffer)
    }
    sc.stop()
  }
}
