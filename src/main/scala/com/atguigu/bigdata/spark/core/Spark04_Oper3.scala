package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jxy on 2020/10/1 0001 12:26
  */
object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc = new SparkContext(sparkConf)
    //map算子
    val listRdd = sc.makeRDD(1.to(10))
    val indexRdd = listRdd.mapPartitionsWithIndex {
      case (num, datas) => {
           datas.map((_,"分区号："+num))
      }
    }
    indexRdd.collect().foreach(println)
  }
}
