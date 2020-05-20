package com.bigdata.jdbc


import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.1.106:3306/spark", "root", "LIXINXIN123!")

    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM location_info where id >= ? AND id <= ?",
      //1,4分别为sql的参数，2为分区数
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    val jrdd = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }

}
