package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1 0001 22:34
  */
object Spark13_Oper12 {
  def main(args: Array[String]): Unit = {
    //重分区
    val config = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    val listRdd = sc.makeRDD(Array(1,2,45,5,2,4))
    val sortRdd = listRdd.sortBy(x=>x,false)
    sortRdd.foreach(println)
  }
}
