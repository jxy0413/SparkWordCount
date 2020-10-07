package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/5 14:27
  */
object Spark_CheckPoint {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val rdd = sc.makeRDD(List(1,2,3,4))
    val mapRdd = rdd.map((_,1))
    //设定检查点的保存目录
    sc.setCheckpointDir("cp")
    mapRdd.checkpoint()
    val reduceBykey = mapRdd.reduceByKey(_+_)
    //reduceBykey.foreach(println)
    println(reduceBykey.toDebugString)
  }
}
