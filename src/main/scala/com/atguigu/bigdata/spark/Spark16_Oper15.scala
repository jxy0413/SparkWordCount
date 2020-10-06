package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/2  0:05
  */
object Spark16_Oper15 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val words = Array("hello","two","one","hello","world")
    val wordPairRdd = sc.makeRDD(words).map(t=>(t,1))
    val reduceRdd = wordPairRdd.reduceByKey(_+_)
    reduceRdd.collect().foreach(println)
  }
}
