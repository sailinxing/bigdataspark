package com.bigdata.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SqlDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SqlDemo")
    //.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "root")
    val rdd = sc.textFile("hdfs://node1:9000/sparkwc/person.txt").map(line => {
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })
    import sqlContext.implicits._
    val personRdd = rdd.toDF
    personRdd.registerTempTable("person")
    sqlContext.sql("select * from person where age >=20 order by age desc limit 2").show()
    sc.stop()
  }
}

case class Person(id: Long, name: String, age: Int)
