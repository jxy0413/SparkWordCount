package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1 0001 23:55
  */
object Spark15_Oper14 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val words = Array("hello","two","one","hello","world")
    val wordPairRdd = sc.makeRDD(words).map(t=>(t,1))
    val groupRdd = wordPairRdd.groupByKey()
    val rdd = groupRdd.map(t=>{(t._1,t._2.size)})
    rdd.foreach(println)
  }
}
