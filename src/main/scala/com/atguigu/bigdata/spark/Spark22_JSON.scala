package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by jxy on 2020/10/5 21:59
  */
object Spark22_JSON {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val json = sc.textFile("file://" + ClassLoader.getSystemResource("User.json"))
    val result = json.map(JSON.parseFull)
    result.foreach(println)
  }
}
