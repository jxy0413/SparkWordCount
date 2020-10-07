package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1  13:38
  */
object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {
    //抽样
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    //分组
    val listRdd = sc.makeRDD(1.to(100))
    //从指定的数据结合中进行抽样处理，根据不同的算法进行抽样
    val vaule = listRdd.sample(false,0.4,1)
    vaule.collect().foreach(println)
  }
}
