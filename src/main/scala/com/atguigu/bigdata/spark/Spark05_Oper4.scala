package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1 12:58
  */
object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    //flatMap
    val listRdd = sc.makeRDD(Array(List(1,2),List(3,4)))
    val faltMapRdd = listRdd.flatMap(x => {
      x
    })
    faltMapRdd.collect().foreach(println)
  }
}
